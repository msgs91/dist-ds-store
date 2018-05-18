package house.exception;

public class ApplicationException extends RuntimeException {
  public ApplicationException(Exception e) {
    super(e);
  }
  
  public ApplicationException(String message) {
    super(message);
  }
}
