package org.apache.pinot.core.startree.v2;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.startree.v2.builder.MultipleTreesBuilder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;

/*******
 * @author: Kaynewu
 * @date: 2021/7/16
 **/
public class StarTreeV2Test {

    private static final Random RANDOM = new Random();
    private static final int NUM_SEGMENT_RECORDS = 100_000;
    private static final int MAX_LEAF_RECORDS = 64;
    private static final String DIMENSION_D1 = "d1";
    private static final String DIMENSION_D2 = "d2";
    private static final int DIMENSION_CARDINALITY = 100;
    private static final String METRIC = "m";


    private static final String TABLE_NAME = "testTable";
    private static final String SEGMENT_NAME = "testSegment";

    public void buildStarTreeIndex() throws Exception {
        File index_dir = new File("D:\\test\\pinot\\startTreeIndex");
        Schema.SchemaBuilder schemaBuilder = new Schema.SchemaBuilder()
                .addSingleValueDimension(DIMENSION_D1, DataType.INT)
                .addSingleValueDimension(DIMENSION_D2, DataType.INT)
                .addMetric(METRIC,DataType.INT);
        Schema schema = schemaBuilder.build();
        TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
        List<GenericRow> segmentRecords = new ArrayList<>(NUM_SEGMENT_RECORDS);
        for (int i = 0; i < NUM_SEGMENT_RECORDS; i++) {
            GenericRow segmentRecord = new GenericRow();
            segmentRecord.putValue(DIMENSION_D1, RANDOM.nextInt(DIMENSION_CARDINALITY));
            segmentRecord.putValue(DIMENSION_D2, RANDOM.nextInt(DIMENSION_CARDINALITY));
            segmentRecord.putValue(METRIC,  RANDOM.nextInt() % 100000);
            segmentRecords.add(segmentRecord);
        }

        SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
        SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
        segmentGeneratorConfig.setOutDir(index_dir.getPath());
        segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
        driver.init(segmentGeneratorConfig, new GenericRowRecordReader(segmentRecords));
        driver.build();


        StarTreeIndexConfig starTreeIndexConfig = new StarTreeIndexConfig(Arrays.asList(DIMENSION_D1, DIMENSION_D2), null,
                Collections.singletonList(
                        new AggregationFunctionColumnPair(AggregationFunctionType.SUM, METRIC).toColumnName()),
                MAX_LEAF_RECORDS);
        File indexDir = new File(index_dir, SEGMENT_NAME);
        // Randomly build star-tree using on-heap or off-heap mode
        MultipleTreesBuilder.BuildMode buildMode =
                RANDOM.nextBoolean() ? MultipleTreesBuilder.BuildMode.ON_HEAP : MultipleTreesBuilder.BuildMode.OFF_HEAP;
        try (MultipleTreesBuilder builder = new MultipleTreesBuilder(Collections.singletonList(starTreeIndexConfig), false,
                indexDir, buildMode)) {
            builder.build();
        }

    }

    public static void main(String[] args){
        try{

        } catch(Throwable e){
            e.printStackTrace();
            System.exit(1);
        }finally{

        }
        System.exit(0);
    }

}
