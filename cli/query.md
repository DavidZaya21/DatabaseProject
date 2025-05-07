
# Queries 


### find all the successor of the nodes

````cassandraql
    select  id from keyspace_name.nodes where  name = '/en/'; 
    select  from_node from keyspace_name.edges where to_node = 'id';
````


### 