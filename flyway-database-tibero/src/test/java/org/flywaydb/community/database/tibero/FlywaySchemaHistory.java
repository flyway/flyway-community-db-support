package org.flywaydb.community.database.tibero;

public class FlywaySchemaHistory {

    private int installedRank;
    private int version;
    private String description;
    private String type;
    private String script;
    private long checkSum;
    private boolean success;

    public FlywaySchemaHistory(int installedRank, int version, String description, String type, String script,
        long checkSum, boolean success) {
        this.installedRank = installedRank;
        this.version = version;
        this.description = description;
        this.type = type;
        this.script = script;
        this.checkSum = checkSum;
        this.success = success;
    }

    public int getInstalledRank() {
        return installedRank;
    }

    public int getVersion() {
        return version;
    }

    public String getDescription() {
        return description;
    }

    public String getType() {
        return type;
    }

    public String getScript() {
        return script;
    }

    public long getCheckSum() {
        return checkSum;
    }

    public boolean isSuccess() {
        return success;
    }

    @Override
    public String toString() {
        return "FlywaySchemaHistory{" +
            "installedRank=" + installedRank +
            ", version=" + version +
            ", description='" + description + '\'' +
            ", type='" + type + '\'' +
            ", script='" + script + '\'' +
            ", checkSum=" + checkSum +
            ", success=" + success +
            '}';
    }
}