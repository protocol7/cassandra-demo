package com.protocol7.demo.cassandra;

import static me.prettyprint.cassandra.utils.StringUtils.bytes;
import static me.prettyprint.cassandra.utils.StringUtils.string;

import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.CassandraClientPool;
import me.prettyprint.cassandra.service.CassandraClientPoolFactory;
import me.prettyprint.cassandra.service.Keyspace;
import org.apache.cassandra.thrift.ColumnPath;


public class Hector {
    
    public static void main(String[] args) {
        CassandraClientPool pool = CassandraClientPoolFactory.INSTANCE.get();
        
        CassandraClient client = pool.borrowClient("localhost", 9160);
        
        Keyspace ks = client.getKeyspace("Kvitter");
        
        String key = "123";

        ColumnPath textColumn = new ColumnPath("kvitters");
        textColumn.setColumn(bytes("text"));
        
        ks.insert(key, textColumn, bytes("Hello world"));

        ColumnPath authorColumn = new ColumnPath("kvitters");
        authorColumn.setColumn(bytes("author"));
        
        ks.insert(key, authorColumn, bytes("Niklas"));
        
        System.out.println(string(ks.getColumn(key, textColumn).getValue()));
        System.out.println(string(ks.getColumn(key, authorColumn).getValue()));
    }
}
