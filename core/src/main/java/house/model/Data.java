package house.model;

public class Data {
  String type;
  String value;
  int version;
  
  public Data() {
  }
  
  public Data(String type, String value, int version) {
    this.type = type;
    this.value = value;
    this.version = version;
  }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }
}