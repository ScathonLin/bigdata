package com.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

/**
 * hbase java api
 * Created by linhd on 2017/10/22.
 */
public class HBaseAPITest {
    private Connection connection = null;
    private Configuration conf = null;
    private Table table = null;
    private Admin admin = null;

    @Before
    public void init() throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        // conf.set("hbase.zookeeper.quorum", "hadoop2,hadoop3,hadoop4");
        conf.set("hbase.zookeeper.quorum", "hadoop2,hadoop3");
        connection = ConnectionFactory.createConnection();
        admin = connection.getAdmin();
    }

    @Test
    public void pageFilter() throws IOException {
        int rowCount = 100;
        int pageSize = rowCount % 3 == 0 ? rowCount / 3 : rowCount / 3 + 1;
        Table table = connection.getTable(TableName.valueOf("human_info"));
        Scan scan = new Scan();
        scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^201810")));
        ResultScanner scanner = table.getScanner(scan);
        int i = 0;
        LinkedList<String> rowKeyList = new LinkedList<>();
        for (Result result : scanner) {
            if (i % pageSize == 0) {
                rowKeyList.add(Bytes.toString(result.getRow()));
            }
            i++;
        }
        System.out.println();
        System.out.println(rowKeyList);
        System.out.println(rowCount);
        System.out.println("=============================");
        rowKeyList.forEach(rowKey -> {
            Scan scan1 = new Scan();
            RowFilter rf1 = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^201810"));
            PageFilter pf1 = new PageFilter(pageSize);
            FilterList fl1 = new FilterList();
            fl1.addFilter(rf1);
            fl1.addFilter(pf1);
            scan1.setFilter(fl1);
            scan1.setStartRow(Bytes.toBytes(rowKey));
            try {
                ResultScanner scanner1 = table.getScanner(scan1);
                scanner1.forEach(result -> {
                    byte[] valueBytes = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("image_path"));
                    byte[] rowBytes = result.getRow();
                    System.out.println("rowkey: " + Bytes.toString(rowBytes) + ", value: " + Bytes.toString(valueBytes));
                });

                System.out.println("============================");

            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    public void rowCount() throws IOException {
        Table table = connection.getTable(TableName.valueOf("human_info"));
        Scan scan = new Scan();
        scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^201810")));
        ResultScanner scanner = table.getScanner(scan);
        int rowCount = 0;
        for (Result result : scanner) {
            rowCount += 1;
        }
        System.out.println(rowCount);
    }

    @Test
    public void rowFilter() throws IOException {
        Table table = connection.getTable(TableName.valueOf("human_info"));
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^201810"));
        PageFilter pageFilter = new PageFilter(20);
        FilterList filterList = new FilterList();
        filterList.addFilter(rowFilter);
        filterList.addFilter(pageFilter);
        Scan scan = new Scan();
//        scan.setStartRow(Bytes.toBytes("201810----26--8d370006-4661-49f6-8065-b904c72b198e"));
        scan.setFilter(filterList);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            byte[] valueBytes = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("image_path"));
            byte[] rowBytes = result.getRow();
            System.out.println("rowkey: " + Bytes.toString(rowBytes) + ", value: " + Bytes.toString(valueBytes));
        }
    }

    @Test
    public void filterTest() throws IOException {
        Table table = connection.getTable(TableName.valueOf("ns1:user"));
        Scan scan = new Scan();
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        SingleColumnValueFilter scvf = new SingleColumnValueFilter(Bytes.toBytes("userInfo"), Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes("linhd"));
        filterList.addFilter(scvf);
        scan.setFilter(filterList);
        scan.addColumn(Bytes.toBytes("userInfo"), Bytes.toBytes("name"));
        scan.addColumn(Bytes.toBytes("userInfo"), Bytes.toBytes("age"));
        ResultScanner scanner = table.getScanner(scan);
        System.out.println(scanner);
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()) {
            Result next = iterator.next();
            String name = Bytes.toString(next.getValue(Bytes.toBytes("userInfo"), Bytes.toBytes("name")));
            String age = Bytes.toString(next.getValue(Bytes.toBytes("userInfo"), Bytes.toBytes("age")));
            System.out.println(String.format("user:{name:%s,age:%s}", name, age));
        }
    }

    @Test
    public void createTable() throws IOException {
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("ns1:address"));
        HColumnDescriptor column1 = new HColumnDescriptor("info1");
        HColumnDescriptor column2 = new HColumnDescriptor("info2");
        tableDescriptor.addFamily(column1);
        tableDescriptor.addFamily(column2);
        admin.createTable(tableDescriptor);
    }

    @Test
    public void updateTable() throws IOException {
        admin.disableTable(TableName.valueOf("human_info"));
        HTableDescriptor descriptor = admin.getTableDescriptor(TableName.valueOf("human_info"));
        descriptor.addFamily(new HColumnDescriptor(Bytes.toBytes("info_new")));
        admin.modifyTable(TableName.valueOf("human_info"), descriptor);
        admin.enableTable(TableName.valueOf("human_info"));
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void scan() throws IOException {
        Scan scan = new Scan(Bytes.toBytes("row1"), Bytes.toBytes("row2"));
        scan.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("province"));
        table = connection.getTable(TableName.valueOf("ns1:address"));
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> scannerIterator = scanner.iterator();
        while (scannerIterator.hasNext()) {
            Result next = scannerIterator.next();
            String province = Bytes.toString(next.getValue(Bytes.toBytes("info1"), Bytes.toBytes("province")));
            String city = Bytes.toString(next.getValue(Bytes.toBytes("info1"), Bytes.toBytes("city")));
            String county = Bytes.toString(next.getValue(Bytes.toBytes("info1"), Bytes.toBytes("county")));
            System.out.println(province + "-" + city + "-" + county);
        }
    }

    @Test
    @SuppressWarnings("Duplicates")
    public void scanner() throws IOException {
        table = connection.getTable(TableName.valueOf("ns1:address"));
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info1"), Bytes
                .toBytes("province"), CompareFilter.CompareOp
                .NOT_EQUAL, Bytes.toBytes("shandong"));
        ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("city"));
//        用的最多的过滤器，有关联的记录都在一起，加快查找效率
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^row1"));
        /**
         * 其他的过滤器
         * ColumnPrefixFilter 列名前缀过滤器
         */
//        ResultScanner scanner = table.getScanner(Bytes.toBytes("info1"));
        //下面三行代码相当于上面一句被注释掉的代码，但是下面的的方法可以设置过滤器
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("info1"));
//        scan.setFilter(filter);
//        scan.setFilter(columnPrefixFilter);
//        scan.setFilter(rowFilter);

        /**
         * FilterList是Filter的子类
         * 同时使用多个过滤器
         */
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        filterList.addFilter(columnPrefixFilter);
        filterList.addFilter(rowFilter);

        scan.setFilter(filterList);
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> scannerIterator = scanner.iterator();
        while (scannerIterator.hasNext()) {
            Result next = scannerIterator.next();
            String province = Bytes.toString(next.getValue(Bytes.toBytes("info1"), Bytes.toBytes("province")));
            String city = Bytes.toString(next.getValue(Bytes.toBytes("info1"), Bytes.toBytes("city")));
            String county = Bytes.toString(next.getValue(Bytes.toBytes("info1"), Bytes.toBytes("county")));
            System.out.println(province + "-" + city + "-" + county);
        }
    }

    @Test
    public void put() throws IOException {
        table = connection.getTable(TableName.valueOf("human_info"));
        Put put = new Put(Bytes.toBytes("201810----0--eafc589a-18a2-4db8-86ac-ce295463ad57"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("city"), Bytes.toBytes("lujiakuang"));
        table.put(put);
    }

    @Test
    public void putBatch() throws IOException {
        table = connection.getTable(TableName.valueOf("human_info"));
        List<Put> putList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Put put = new Put(Bytes.toBytes("201806--" + "--" + i + "--" + UUID.randomUUID().toString()));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("image_path"), Bytes.toBytes("http://www.baidu.com"));
            putList.add(put);
        }
//        Put put1 = new Put(Bytes.toBytes("row10"));
//        put1.addColumn(Bytes.toBytes("info2"), Bytes.toBytes("county"), Bytes.toBytes("rushan"));
//        Put put2 = new Put(Bytes.toBytes("row9"));
//        put2.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("county"), Bytes.toBytes("rushan"));
//        List<Put> puts = new ArrayList<>();
//        puts.add(put1);
//        puts.add(put2);
//        table.put(puts);
        table.put(putList);
    }

    @Test
    public void get() throws IOException {
        table = connection.getTable(TableName.valueOf("ns1:address"));
        Get get = new Get(Bytes.toBytes("row1"));
        Result result = table.get(get);
        System.out.println(new StringBuilder().append(Bytes.toString(result.getValue(Bytes.toBytes("info1"), Bytes
                .toBytes("province"))))
                .append(Bytes.toString(result.getValue(Bytes.toBytes("info9"), Bytes.toBytes("city"))))
                .append(Bytes.toString(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("county")))).toString());
        ;
    }

    @Test
    public void testDelete() throws IOException {
        table = connection.getTable(TableName.valueOf("ns1:user"));
        Delete delete = new Delete(Bytes.toBytes("row1"));
        //如果不指定要删除的具体信息，那么会将整行删除，如果如果将下面的一行代码注释掉，那么row_key是row1的这一行将会被全部删除
//        delete.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("name"));
        table.delete(delete);
    }

    @After
    public void after() throws IOException {
        if (table != null) {
            table.close();
        }
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
