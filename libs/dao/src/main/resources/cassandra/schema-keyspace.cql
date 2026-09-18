CREATE KEYSPACE IF NOT EXISTS thingsboard
WITH replication = {
	'class' : 'SimpleStrategy',
	'replication_factor' : 1
};
