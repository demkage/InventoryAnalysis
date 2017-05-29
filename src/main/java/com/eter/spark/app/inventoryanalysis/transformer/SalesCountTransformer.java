package com.eter.spark.app.inventoryanalysis.transformer;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.count;

/**
 * Created by abosii on 5/24/2017.
 */
public class SalesCountTransformer extends Transformer {
    private static final long serialVersionUID = 5996538160505838673L;
    private String inputCol = "productid";
    private String outputCol = "sales";
    private String dateCol = "saledate";

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        Dataset<Row> salesCount = dataset.groupBy(dataset.col(inputCol))
                .count().withColumnRenamed("count", outputCol);

        salesCount = dataset.join(salesCount, salesCount.col(inputCol).equalTo(dataset.col(inputCol)))
                .drop(salesCount.col(inputCol));

        return salesCount;
    }

    @Override
    public Transformer copy(ParamMap paramMap) {
        return this;
    }

    @Override
    public StructType transformSchema(StructType structType) {
        structType.add(outputCol, DataTypes.IntegerType);
        return structType;
    }

    public String uid() {
        return String.valueOf(serialVersionUID);
    }

    public String getInputCol() {
        return inputCol;
    }

    public SalesCountTransformer setInputCol(String inputCol) {
        this.inputCol = inputCol;
        return this;
    }

    public String getOutputCol() {
        return outputCol;
    }

    public SalesCountTransformer setOutputCol(String outputCol) {
        this.outputCol = outputCol;
        return this;
    }

    public String getDateCol() {
        return dateCol;
    }

    public SalesCountTransformer setDateCol(String dateCol) {
        this.dateCol = dateCol;
        return this;
    }
}
