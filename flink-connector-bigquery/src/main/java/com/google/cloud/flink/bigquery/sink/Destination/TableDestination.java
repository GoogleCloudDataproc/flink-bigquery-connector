package com.google.cloud.flink.bigquery.sink.Destination;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.storage.v1.TableSchema;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TableDestination implements Serializable {
    private final String tableSpec;

    /**
     * Project IDs must contain 6-63 lowercase letters, digits, or dashes. IDs must start with a
     * letter and may not end with a dash. This regex isn't exact - this allows for patterns that
     * would be rejected by the service, but this is sufficient for basic parsing of table references.
     */
    private static final String PROJECT_ID_REGEXP = "[a-z][-a-z0-9:.]{4,61}[a-z0-9]";

    /** Regular expression that matches Dataset IDs. */
    private static final String DATASET_REGEXP = "[-\\w.]{1,1024}";

    /** Regular expression that matches Table IDs. */
    private static final String TABLE_REGEXP = "[-\\w$@ ]{1,1024}";

    /**
     * Matches table specifications in the form {@code "[project_id]:[dataset_id].[table_id]"} or
     * {@code "[dataset_id].[table_id]"}.
     */
    private static final String DATASET_TABLE_REGEXP =
            String.format(
                    "((?<PROJECT>%s)[:\\.])?(?<DATASET>%s)\\.(?<TABLE>%s)",
                    PROJECT_ID_REGEXP, DATASET_REGEXP, TABLE_REGEXP);

    static final Pattern TABLE_SPEC = Pattern.compile(DATASET_TABLE_REGEXP);

    public static String toTableSpec(TableReference ref) {
        StringBuilder sb = new StringBuilder();
        if (ref.getProjectId() != null) {
            sb.append(ref.getProjectId());
            sb.append(":");
        }

        sb.append(ref.getDatasetId()).append('.').append(ref.getTableId());
        return sb.toString();
    }

    public static TableReference parseTableSpec(String tableSpec) {
        Matcher match = TABLE_SPEC.matcher(tableSpec);
        if (!match.matches()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Table specification [%s] is not in one of the expected formats ("
                                    + " [project_id]:[dataset_id].[table_id],"
                                    + " [project_id].[dataset_id].[table_id],"
                                    + " [dataset_id].[table_id])",
                            tableSpec));
        }

        TableReference ref = new TableReference();
        ref.setProjectId(match.group("PROJECT"));

        return ref.setDatasetId(match.group("DATASET")).setTableId(match.group("TABLE"));
    }
    public TableDestination(TableReference tableReference){
        this.tableSpec = toTableSpec(tableReference);
    }
    // ToDO: Make constructors.
    public TableReference getTableReference(){
        return parseTableSpec(this.tableSpec);

    }
}
