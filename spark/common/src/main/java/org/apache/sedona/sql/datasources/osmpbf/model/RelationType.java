package org.apache.sedona.sql.datasources.osmpbf.model;

public enum RelationType {
    NODE,
    WAY,
    RELATION;

    public static String fromValue(int number) {
        switch (number) {
            case 0:
                return NODE.toString();
            case 1:
                return WAY.toString();
            case 2:
                return RELATION.toString();
            default:
                throw new IllegalArgumentException("Unknown relation type: " + number);
        }
    }
}
