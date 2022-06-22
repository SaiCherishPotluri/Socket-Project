import java.io.Serializable;

public class Record implements Serializable {
    private static final long serialVersionUID = 8829136421241571165L;

    String countryCode;
    String shortName;
    String tableName;
    String longName;
    String alphaCode;
    String currencyUnit;
    String region;
    String code;
    String census;

    public Record(String countryCode, String shortName, String tableName, String longName,
                  String alphaCode, String currencyUnit, String region, String code, String census) {
        this.countryCode = countryCode;
        this.shortName = shortName;
        this.tableName = tableName;
        this.longName = longName;
        this.alphaCode = alphaCode;
        this.currencyUnit = currencyUnit;
        this.region = region;
        this.code = code;
        this.census = census;
    }

    public String getCountryCode() {
        return countryCode;
    }

    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }

    public String getShortName() {
        return shortName;
    }

    public void setShortName(String shortName) {
        this.shortName = shortName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getLongName() {
        return longName;
    }

    public void setLongName(String longName) {
        this.longName = longName;
    }

    public String getAlphaCode() {
        return alphaCode;
    }

    public void setAlphaCode(String alphaCode) {
        this.alphaCode = alphaCode;
    }

    public String getCurrencyUnit() {
        return currencyUnit;
    }

    public void setCurrencyUnit(String currencyUnit) {
        this.currencyUnit = currencyUnit;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getCensus() {
        return census;
    }

    public void setCensus(String census) {
        this.census = census;
    }

    @Override
    public String toString() {
        return "Record{" +
                "countryCode='" + countryCode + '\'' +
                ", shortName='" + shortName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", longName='" + longName + '\'' +
                ", alphaCode='" + alphaCode + '\'' +
                ", currencyUnit='" + currencyUnit + '\'' +
                ", region='" + region + '\'' +
                ", code='" + code + '\'' +
                ", census='" + census + '\'' +
                '}';
    }
}
