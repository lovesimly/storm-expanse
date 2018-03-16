package com.gtexpanse.app.expanse.bean;

public enum DBMSAction {
    INSERT('I', 0),
    DELETE('D', 1),
    UPDATE('U', 2);

    private char shotName;
    private int value;

    DBMSAction(char shotName, int value) {
        this.shotName = shotName;
        this.value = value;
    }

    public static char getShotName(int value) {
        char re = '\0';
        for (DBMSAction dbmsAction : DBMSAction.values()) {
            if (dbmsAction.value == value) {
                re = dbmsAction.getShotName();
                break;
            }
        }
        return re;
    }

    public Integer getValue() {
        return value;
    }

    public char getShotName() {
        return shotName;
    }
}
