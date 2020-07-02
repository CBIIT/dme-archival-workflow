package gov.nih.nci.hpc.dmesync.domain;

import javax.persistence.*;

@Entity
public class PermissionBookmarkInfo {
  @Id @GeneratedValue private Long id;
  private String path;
  private String userId;
  private String permission;
  private String createBookmark;
  private String created;
  private String error;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String getUserId() {
    return userId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public String getPermission() {
    return permission;
  }

  public void setPermission(String permission) {
    this.permission = permission;
  }

  public String getCreateBookmark() {
    return createBookmark;
  }

  public void setCreateBookmark(String createBookmark) {
    this.createBookmark = createBookmark;
  }

  public String getCreated() {
    return created;
  }

  public void setCreated(String created) {
    this.created = created;
  }

  public String getError() {
    return error;
  }

  public void setError(String error) {
    this.error = error;
  }
}
