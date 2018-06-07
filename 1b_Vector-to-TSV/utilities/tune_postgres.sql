ALTER SYSTEM SET
 max_connections = '200';
ALTER SYSTEM SET
 shared_buffers = '64GB';
ALTER SYSTEM SET
 effective_cache_size = '192GB';
ALTER SYSTEM SET
 maintenance_work_mem = '2GB';
ALTER SYSTEM SET
 checkpoint_completion_target = '0.9';
ALTER SYSTEM SET
 wal_buffers = '16MB';
ALTER SYSTEM SET
 default_statistics_target = '500';
ALTER SYSTEM SET
 random_page_cost = '2';
ALTER SYSTEM SET
 effective_io_concurrency = '2';
ALTER SYSTEM SET
 work_mem = '16MB';
ALTER SYSTEM SET
 min_wal_size = '4GB';
ALTER SYSTEM SET
 max_wal_size = '8GB';
ALTER SYSTEM SET
 max_worker_processes = '64';
ALTER SYSTEM SET
 max_parallel_workers_per_gather = '32';
ALTER SYSTEM SET
 max_parallel_workers = '64';
ALTER SYSTEM SET
 max_wal_size = '288MB'
