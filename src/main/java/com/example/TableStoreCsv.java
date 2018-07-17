package com.example;

import com.alicloud.openservices.tablestore.model.RowPutChange;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

public class TableStoreCsv implements Iterable<RowPutChange> {
    private static final String DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
    private BufferedReader br;
    private TableStoreCsvParser parser;

    TableStoreCsv(String resourceCsvName, TableStoreCsvParser parser) throws IOException {
        InputStream is = ClassLoader.getSystemResourceAsStream(resourceCsvName);
        br = new BufferedReader(new InputStreamReader(is));

        String firstLine = br.readLine();
        String[] cols = firstLine.split(DELIMITER, -1);

        this.parser = parser;
        this.parser.parseHeaderRow(cols);
    }

    public Iterator<RowPutChange> iterator() {
        return new MyIterator();
    }

    class MyIterator implements Iterator<RowPutChange> {
        private String line;

        public boolean hasNext() {
            try {
                line = br.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return line != null;
        }

        public RowPutChange next() {
            String[] cols = line.split(DELIMITER, -1);
            return parser.genRowPutChange(cols);
        }

        public void remove() {
            throw new UnsupportedOperationException("not supported yet");
        }
    }
}
