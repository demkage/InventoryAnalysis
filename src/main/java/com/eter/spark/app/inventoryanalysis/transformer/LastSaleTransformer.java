package com.eter.spark.app.inventoryanalysis.transformer;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.time.LocalDate;

import static org.apache.spark.sql.functions.current_date;
import static org.apache.spark.sql.functions.datediff;

/**
 * Created by abosii on 5/24/2017.
 */
public class LastSaleTransformer extends Transformer {
    private static final long serialVersionUID = -7325215370315061692L;

    private String productCol = "productid";
    private String inputCol = "saledate";
    private String outputCol = "lastsale";

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        Dataset<Row> result = dataset
                .withColumn(outputCol, datediff(current_date(), dataset.col(inputCol)));

        result = result.groupBy(result.col(productCol)).min(outputCol).withColumnRenamed("min(" + outputCol + ")", outputCol);

        return dataset.join(result, dataset.col(productCol).equalTo(result.col(productCol)))
                .drop(result.col(productCol));
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

    public String getProductCol() {
        return productCol;
    }

    public LastSaleTransformer setProductCol(String productCol) {
        this.productCol = productCol;
        return this;
    }

    public String getInputCol() {
        return inputCol;
    }

    public LastSaleTransformer setInputCol(String inputCol) {
        this.inputCol = inputCol;
        return this;
    }

    public String getOutputCol() {
        return outputCol;
    }

    public LastSaleTransformer setOutputCol(String outputCol) {
        this.outputCol = outputCol;
        return this;
    }
}
