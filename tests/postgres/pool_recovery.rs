use sqlx::{postgres::PgPoolOptions, Connection, Executor, PgConnection, PgPool};
use std::time::Duration;

async fn pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(1)
        .test_before_acquire(false)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap()
}

async fn healthy(pool: &PgPool) {
    let mut conn = pool.acquire().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT 42")
            .fetch_one(&mut *conn)
            .await
            .unwrap(),
        42
    );
    assert!(!conn.is_in_transaction());
    let tx = conn.begin().await.unwrap();
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn pool_discards_untracked_aborted_transaction_after_fk_error() {
    let pool = pool().await;
    let mut conn = pool.acquire().await.unwrap();
    conn.execute("CREATE TEMP TABLE parent (id int PRIMARY KEY); CREATE TEMP TABLE child (id int REFERENCES parent(id)); BEGIN").await.unwrap();
    let err = conn
        .execute("INSERT INTO child VALUES (1)")
        .await
        .unwrap_err();
    assert_eq!(
        err.as_database_error().unwrap().code().as_deref(),
        Some("23503")
    );
    // No SQLx Transaction owns this server transaction. Protocol Sync succeeds
    // even here, so the old on-release ping recycled this poisoned connection.
    drop(conn);
    healthy(&pool).await;
    pool.close().await;
}

#[tokio::test]
async fn pool_discards_untracked_open_transaction() {
    let pool = pool().await;
    let mut conn = pool.acquire().await.unwrap();
    conn.execute("BEGIN").await.unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    drop(conn);
    healthy(&pool).await;
    let next_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_ne!(
        pid, next_pid,
        "an unowned open transaction must be discarded"
    );
    pool.close().await;
}

#[tokio::test]
async fn pool_recovers_after_failed_begin() {
    let pool = pool().await;
    let mut conn = pool.acquire().await.unwrap();
    assert!(conn.begin_with("BEGIN; SELECT 1/0").await.is_err());
    drop(conn);
    healthy(&pool).await;
    pool.close().await;
}

#[tokio::test]
async fn cancelled_begin_is_rolled_back_before_connection_reuse() {
    let pool = pool().await;
    let mut conn = pool.acquire().await.unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    let mut observer = PgConnection::connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    {
        let begin = conn.begin_with("BEGIN; SELECT pg_sleep(0.5)");
        tokio::pin!(begin);
        tokio::select! {
            result = &mut begin => panic!("BEGIN completed before cancellation: {result:?}"),
            _ = async {
                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        let sleeping: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1 AND wait_event = 'PgSleep')")
                            .bind(pid).fetch_one(&mut observer).await.unwrap();
                        if sleeping { break; }
                        tokio::task::yield_now().await;
                    }
                }).await.unwrap();
            } => {}
        }
    }
    // Check the borrowed connection too: a cancellation must queue rollback
    // even when the connection is not immediately returned to a pool.
    conn.ping().await.unwrap();
    assert!(!conn.is_in_transaction());
    let state: String = sqlx::query_scalar("SELECT state FROM pg_stat_activity WHERE pid = $1")
        .bind(pid)
        .fetch_one(&mut observer)
        .await
        .unwrap();
    assert_eq!(
        state, "idle",
        "cancelled BEGIN left a server transaction open"
    );
    drop(conn);
    healthy(&pool).await;
    pool.close().await;
}

#[tokio::test]
async fn ordinary_failed_transaction_rolls_back_and_reuses_connection() {
    let pool = pool().await;
    let mut conn = pool.acquire().await.unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    let mut tx = conn.begin().await.unwrap();
    assert!(tx.execute("SELECT 1/0").await.is_err());
    drop(tx);
    drop(conn);
    healthy(&pool).await;
    let next_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        pid, next_pid,
        "ordinary rollback should preserve healthy connections"
    );
    pool.close().await;
}
