package com.asif.kafka;

import com.asif.kafka.model.Issue;
import com.asif.kafka.model.PullRequest;
import com.asif.kafka.model.User;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.asif.kafka.GitHubSchemas.*;

public class GitHubSourceTask extends SourceTask {
  /*
    Your connector should never use System.out for logging. All of your classes should use slf4j
    for logging
 */
  static final Logger log = LoggerFactory.getLogger(GitHubSourceTask.class);

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  @Override
  public void start(Map<String, String> map) {
    //TODO: Do things here that are required to start your task. This could be open a connection to a database, etc.
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    //TODO: Create SourceRecord objects that will be sent the kafka cluster.
    throw new UnsupportedOperationException("This has not been implemented.");
  }

  @Override
  public void stop() {
    //TODO: Do whatever is required to stop your task.
  }

  public Struct buildRecordValue(Issue issue){

    // Issue top level fields
    Struct valueStruct = new Struct(VALUE_SCHEMA)
            .put(URL_FIELD, issue.getUrl())
            .put(TITLE_FIELD, issue.getTitle())
            .put(CREATED_AT_FIELD, Date.from(issue.getCreatedAt()))
            .put(UPDATED_AT_FIELD, Date.from(issue.getUpdatedAt()))
            .put(NUMBER_FIELD, issue.getNumber())
            .put(STATE_FIELD, issue.getState());

    // User is mandatory
    User user = issue.getUser();
    Struct userStruct = new Struct(USER_SCHEMA)
            .put(USER_URL_FIELD, user.getUrl())
            .put(USER_ID_FIELD, user.getId())
            .put(USER_LOGIN_FIELD, user.getLogin());
    valueStruct.put(USER_FIELD, userStruct);

    // Pull request is optional
    PullRequest pullRequest = issue.getPullRequest();
    if (pullRequest != null) {
      Struct prStruct = new Struct(PR_SCHEMA)
              .put(PR_URL_FIELD, pullRequest.getUrl())
              .put(PR_HTML_URL_FIELD, pullRequest.getHtmlUrl());
      valueStruct.put(PR_FIELD, prStruct);
    }

    return valueStruct;
  }}
