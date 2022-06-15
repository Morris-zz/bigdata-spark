import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author: zhaozheng
 * @date: 2022/6/15 11:05 上午
 * @description:
 */
public class HbaseOperationTest {
    private Connection connection;

    @Before
    public void beforeTest() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        connection = ConnectionFactory.createConnection(configuration);
    }

    @After
    public void afterTest() throws IOException {
        connection.close();
    }

    @Test
    public void tableTest() throws IOException {
        Admin admin = connection.getAdmin();
        TableName tbName = TableName.valueOf("SHEIN_GOODS_TAG");
        if (admin.tableExists(tbName)){
            return;
        }
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("SHEIN_GOODS_TAG"));
        tableDescriptor.addFamily(new HColumnDescriptor("c1"));
        admin.createTable(tableDescriptor);
    }


    @Test
    public void putRowKeyTest() throws IOException {
        TableName tbName = TableName.valueOf("SHEIN_GOODS_TAG");
        Table table = connection.getTable(tbName);
        String rowKey = "abc_600014";
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("c1"),Bytes.toBytes("name"),Bytes.toBytes("zz"));
        put.addColumn(Bytes.toBytes("c1"),Bytes.toBytes("age"),Bytes.toBytes("28"));
        put.addColumn(Bytes.toBytes("c1"),Bytes.toBytes("cool"),Bytes.toBytes("100/100"));
        put.addColumn(Bytes.toBytes("c1"),Bytes.toBytes("rich"),Bytes.toBytes("100/100"));
        table.put(put);
        table.close();

    }


    @Test
    public void getRowKeyTest() throws IOException {
        TableName tbName = TableName.valueOf("SHEIN_GOODS_TAG");
        Table table = connection.getTable(tbName);
        Get get = new Get(Bytes.toBytes("abc_600014"));
        Result result = table.get(get);
        List<Cell> cells = result.listCells();
        byte[] row = result.getRow();
        System.out.println(Bytes.toString(row));
        for (Cell cell : cells) {
            String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
            String col = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            String rowName = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
            String val = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            System.out.println("cf:"+family+" col:"+col+" row:"+rowName+" val:"+val);

        }

        table.close();

    }


}
