CREATE KEYSPACE IF NOT EXISTS jnks_iot
WITH replication = {
	'class' : 'SimpleStrategy',
	'replication_factor' : 1
};
