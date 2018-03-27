package com.intrence.cdg.persistence;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

import com.intrence.config.ConfigProvider;
import com.intrence.config.collection.ConfigMap;
import com.intrence.cdg.task.RuleRecurrence;
import com.intrence.cdg.task.TaskBatchRequest;
import com.intrence.cdg.task.TaskRule;
import com.intrence.cdg.task.TaskRun;
import com.intrence.cdg.util.Constants;
import com.intrence.models.model.SearchParams;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.flywaydb.core.Flyway;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

//TODO: Switch to Spring-JDBC or some other database abstraction library to handle db connections.
//TODO: Refer: https://github.groupondev.com/voltron/abacus/blob/master/application/src/main/java/com/groupon/merchantdata/abacus/persistence/quasar/QuasarDAO.java#L218-L226

@Component
public class PostgresQueryService {

    protected final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final ConfigMap CONFIG_MAP = ConfigProvider.getConfig();

    private static final String PG_HOST = CONFIG_MAP.getString("postgres.pg_host");
    private static final String PG_DB = CONFIG_MAP.getString("postgres.pg_database");

    private static final int PG_APP_PORT = CONFIG_MAP.getInteger("postgres.pg_app_port");
    private static final String PG_APP_USER = CONFIG_MAP.getString("postgres.pg_app_user");
    private static final String PG_APP_PSWD = CONFIG_MAP.getString("postgres.pg_app_pswd");

    private static final int PG_DBA_PORT = CONFIG_MAP.getInteger("postgres.pg_dba_port");
    private static final String PG_DBA_USER = CONFIG_MAP.getString("postgres.pg_dba_user");
    private static final String PG_DBA_PSWD = CONFIG_MAP.getString("postgres.pg_dba_pswd");

    public static final List<String> DEV_ENVIRONMENTS = ImmutableList.of("testing", "integration");

    private static final Logger LOGGER = Logger.getLogger(PostgresQueryService.class);

    private ComboPooledDataSource comboPooledDataSource;

    private static PostgresQueryService INSTANCE;

    @PostConstruct
    public void init() throws Exception {
        //In dev environments we cannot use postgres as postgres GDS does not allow connection from random CI / development hosts
        if (!DEV_ENVIRONMENTS.contains(ConfigProvider.getEnvironment())) {
            LOGGER.debug("Start initializing Flyway...");
            Flyway flyway = new Flyway();
            LOGGER.debug("Initializing Flyway and setting the data source...");
            flyway.setDataSource(String.format("jdbc:postgresql://%s:%s/%s?prepareThreshold=0", PG_HOST, PG_DBA_PORT, PG_DB), PG_DBA_USER, PG_DBA_PSWD);
            LOGGER.debug(String.format("Initializing the flyway db migration with db details: " +
                                       "pg_host=%s, pg_dba_port=%d, pg_database=%s, pg_dba_user=%s", PG_HOST, PG_DBA_PORT, PG_DB, PG_DBA_USER));
            flyway.migrate();
            LOGGER.debug("Flyway migrated the database successfully.");

            comboPooledDataSource = new ComboPooledDataSource();
            LOGGER.info("Event=Loading JDBC postgresql driver...");
            comboPooledDataSource.setDriverClass("org.postgresql.Driver"); //loads the jdbc driver
            comboPooledDataSource.setJdbcUrl(String.format("jdbc:postgresql://%s:%s/%s?prepareThreshold=0", PG_HOST, PG_APP_PORT, PG_DB));
            comboPooledDataSource.setUser(PG_APP_USER);
            comboPooledDataSource.setPassword(PG_APP_PSWD);
            LOGGER.info(String.format("Event=JDBC postgresql driver initialized with pg_host=%s, pg_port=%d, pg_database=%s, pg_user=%s",
                                      PG_HOST, PG_APP_PORT, PG_DB, PG_APP_USER));
        }
    }

    // DEPRECATED - This will be deprecated in very near future and will taken care by spring DI
    @Deprecated
    public static PostgresQueryService getInstance() throws Exception {
        if (INSTANCE == null) {
            synchronized (PostgresQueryService.class) {
                if (INSTANCE == null) {
                    INSTANCE = new PostgresQueryService();
                    try {
                        INSTANCE.init();
                    } catch (Exception e) {
                        LOGGER.error("Error instantiating PostgresQueryService", e);
                        throw e;
                    }
                }
            }
        }
        return INSTANCE;
    }


    @PreDestroy
    public void shutdown() {
        comboPooledDataSource.close();
    }

    public Connection getConnection() throws SQLException {
        return comboPooledDataSource.getConnection();
    }

    /**
     * Returns set of task-rules for given set of sources with given set of statuses
     *
     * @param sources
     * @param statuses
     * @return
     */
    public Set<TaskRule> getTaskRules(Set<String> sources, Set<TaskRule.Status> statuses) {
        PreparedStatement st = null;
        ResultSet rs = null;
        Connection db = null;
        Set<TaskRule> taskRuleSet = new HashSet<>();
        try {
            long current = System.currentTimeMillis();
            db = comboPooledDataSource.getConnection();
            String query = String.format("SELECT * FROM task_rules WHERE %s AND %s", getORPredicate(sources, "source"), getORPredicate(statuses, "status"));
            st = db.prepareStatement(query);
            LOGGER.info(String.format("Event=PostgresSelect query=%s timeTaken=%s", query, (System.currentTimeMillis() - current)));

            rs = st.executeQuery();

            while (rs.next()) {
                taskRuleSet.add(convertToTaskRule(rs));
            }
        } catch (IllegalArgumentException e) {
            LOGGER.error("Exception=empty_source_list while getting the task_rule with status=RUNNABLE. Message : " + e.getMessage());
        } catch (Exception e) {
            LOGGER.error("Exception=PostgresException error while getting the task_rule with status=RUNNABLE", e);
            throw new RuntimeException(e);
        } finally {
            closeResultStatementsConnections(rs, st, db);
        }
        return taskRuleSet;
    }

    protected <T> String getORPredicate(Set<T> sources, String key) {
        if (sources.size() == 0) {
            throw new IllegalArgumentException("Cannot construct source predicate for empty sources list");
        }

        if (sources.size() == 1) {
            for (T source : sources) {
                return String.format("( %s='%s' )", key, source);
            }
        }


        StringBuilder predicateBuilder = new StringBuilder();
        int i = 0;
        for (T source : sources) {
            if (i == 0) {
                predicateBuilder.append(String.format("( %s='%s'", key, source));
            } else if (i == sources.size() - 1) {
                predicateBuilder.append(String.format(" OR %s='%s' )", key, source));
            } else {
                predicateBuilder.append(String.format(" OR %s='%s'", key, source));
            }
            i++;
        }
        return predicateBuilder.toString();
    }

    /**
     * @param rule
     * @return TaskRule object after insertion (this will have id on it)
     */
    public TaskRule addRule(TaskRule rule) {
        PreparedStatement st = null;
        ResultSet rs = null;
        Connection db = null;
        try {

            db = comboPooledDataSource.getConnection();
            String query = String.format("INSERT INTO task_rules (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                                         + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", TaskRule.STATUS, TaskRule.SEARCH_PARAMS,
                                         TaskRule.RECURRENCE, TaskRule.NEXT_SCHEDULE_AT, TaskRule.CREATED_AT, TaskRule.UPDATED_AT, TaskRule.SOURCE,
                                         TaskRule.TYPE, TaskRule.LAST_UPDATED_BY, TaskRule.MAX_RECORDS, TaskRule.CREATOR_ID, TaskRule.CLIENT_ID,
                                         TaskRule.TASK_BUCKET, TaskRule.COUNTRY, TaskRule.AUTO_START);

            long current = System.currentTimeMillis();
            st = db.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            st.setString(1, rule.getStatus().toString());
            st.setString(2, toJson(rule.getSearchParams()));
            st.setString(3, toJson(rule.getRecurrence()));
            if (rule.getRecurrence() != null && rule.getRecurrence().useCronJob()) {
                st.setTimestamp(4, rule.getRecurrence().getCronExpression().nextValidTime());
            } else {
                st.setTimestamp(4, getDef(rule.getNextScheduleAt(), new Timestamp(current)));
            }
            st.setTimestamp(5, getDef(rule.getCreatedAt(), new Timestamp(current)));
            st.setTimestamp(6, getDef(rule.getUpdatedAt(), new Timestamp(current)));
            st.setString(7, rule.getSource());
            st.setString(8, rule.getType());
            st.setString(9, rule.getLastUpdatedBy());
            st.setInt(10, rule.getMaxRecords());
            st.setString(11, rule.getCreatorId());
            st.setString(12, rule.getClientId());
            st.setString(13, rule.getTaskBucket());
            st.setString(14, rule.getCountry());
            st.setBoolean(15, rule.getAutoStart());
            st.executeUpdate();

            rs = st.getGeneratedKeys();
            TaskRule addedRule = null;
            if (rs.next()) {
                int id = rs.getInt(TaskRule.ID);
                addedRule = getRule(id);
            }

            if (addedRule == null) {
                throw new RuntimeException("Exception=PostgresException something wrong with adding a new TaskRule, "
                                           + "the task rule was added but unable to fetch its id after insert");
            }

            return addedRule;

        } catch (Exception e) {
            LOGGER.error("Exception=PostgresException error while adding a new TaskRule", e);
            throw new RuntimeException(e);
        } finally {
            closeResultStatementsConnections(rs, st, db);
        }
    }

    public TaskRule getRule(int id) {
        PreparedStatement st = null;
        ResultSet resultSet = null;
        Connection db = null;
        try {
            db = comboPooledDataSource.getConnection();
            String query = String.format("SELECT * from task_rules where %s=?", TaskRule.ID);
            st = db.prepareStatement(query);
            st.setInt(1, id);
            resultSet = st.executeQuery();

            if (resultSet.next()) {
                return convertToTaskRule(resultSet);
            } else {
                LOGGER.error(String.format("TaskRule with id %s not found", id));
                return null;
            }

        } catch (Exception e) {
            LOGGER.error(String.format("Exception=PostgresException error while getting TaskRule with id %s", id), e);
            throw new RuntimeException(e);
        } finally {
            closeResultStatementsConnections(resultSet, st, db);
        }
    }

    public boolean createTaskBatchRequests(List<TaskBatchRequest> batchRequests) {
        PreparedStatement preparedStatement = null;
        Connection db = null;
        try {
            db = comboPooledDataSource.getConnection();
            db.setAutoCommit(false);
            long current = System.currentTimeMillis();
            Timestamp currentTs = new Timestamp(current);

            String query = String.format("INSERT INTO task_batch_request (%s, %s, %s, %s, %s) "
                                         + "VALUES (?, ?, ?, ?, ?)", TaskBatchRequest.SOURCE, TaskBatchRequest.CREATED_AT, TaskBatchRequest.UPDATED_AT,
                                         TaskBatchRequest.STATUS, TaskBatchRequest.FETCH_REQUEST);
            preparedStatement = db.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);

            for (TaskBatchRequest taskBatchRequest : batchRequests) {
                preparedStatement.setString(1, taskBatchRequest.getSource());
                preparedStatement.setTimestamp(2, currentTs);
                preparedStatement.setTimestamp(3, currentTs);
                preparedStatement.setString(4, taskBatchRequest.getStatus());
                preparedStatement.setString(5, taskBatchRequest.getFetchRequestJsonString());
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            db.commit();
        } catch (SQLException e) {
            LOGGER.error("Exception=PostgresException error while creating batch task record", e);
            return false;
        } finally {
            closeResultStatementsConnections(null, preparedStatement, db);
        }

        return true;
    }

    public void updateTaskBatchRequest(int id, TaskBatchRequest.Status status) {
        PreparedStatement st = null;
        Connection db = null;
        try {
            db = comboPooledDataSource.getConnection();
            String query = String.format("UPDATE task_batch_request SET (%s) = (?),(%s) = (?) WHERE (%s) = (?)",
                                         TaskBatchRequest.STATUS, TaskBatchRequest.UPDATED_AT, TaskBatchRequest.ID);
            long current = System.currentTimeMillis();
            Timestamp currentTs = new Timestamp(current);
            st = db.prepareStatement(query);
            st.setString(1, status.toString());
            st.setTimestamp(2, currentTs);
            st.setInt(3, id);
            int row_updated = st.executeUpdate();
            if (row_updated != 0) {
                LOGGER.info(String.format("Event=PostgresUpdate: TaskBatchRequest with  id=%d marked status=%s.", id, status));
            }

        } catch (SQLException e) {
            LOGGER.error("Exception=PostgresException error while updating task rule status", e);
        } finally {
            closeResultStatementsConnections(null, st, db);
        }
    }

    public List<TaskBatchRequest> getTaskBatchRequestBySource(TaskBatchRequest.Status status) {
        PreparedStatement st = null;
        ResultSet resultSet = null;
        Connection db = null;
        List<TaskBatchRequest> batchRequests = new ArrayList<>();
        try {
            db = comboPooledDataSource.getConnection();
            String query = String.format("SELECT * from task_batch_request WHERE %s = (?)", TaskBatchRequest.STATUS);
            st = db.prepareStatement(query);
            st.setString(1, status.toString());
            resultSet = st.executeQuery();

            while (resultSet.next()) {
                batchRequests.add(convertToTaskBatchRequest(resultSet));
            }
        } catch (SQLException e) {
            LOGGER.error(String.format("Exception=PostgresException error while getting TaskBatchRequest."), e);
        } finally {
            closeResultStatementsConnections(resultSet, st, db);
        }
        return batchRequests;
    }

    public TaskRule getRuleBySource(String source, Set<TaskRule.Status> statuses) {
        PreparedStatement st = null;
        ResultSet resultSet = null;
        Connection db = null;
        try {
            db = comboPooledDataSource.getConnection();
            String query = String.format("SELECT * FROM task_rules WHERE %s = (?) AND %s", TaskRule.SOURCE, getORPredicate(statuses, "status"));
            st = db.prepareStatement(query);
            st.setString(1, source);
            resultSet = st.executeQuery();

            while (resultSet.next()) {
                TaskRule rule = convertToTaskRule(resultSet);
                Long currentTime = System.currentTimeMillis();
                if (!rule.getStatus().equals(TaskRule.Status.FINISHED) ||
                    rule.getNextScheduleAt() != null &&
                    rule.getNextScheduleAt().getTime() < currentTime) {
                    return rule;
                }
            }
            LOGGER.error(String.format("TaskRule with source %s not found", source));
            return null;
        } catch (SQLException | IOException e) {
            LOGGER.error(String.format("Exception=PostgresException error while getting TaskRule with id %s", source), e);
            throw new RuntimeException(e);
        } finally {
            closeResultStatementsConnections(resultSet, st, db);
        }
    }

    public int getRulesCount() {
        PreparedStatement st = null;
        ResultSet resultSet = null;
        Connection db = null;
        int count = 0;
        try {
            db = comboPooledDataSource.getConnection();
            String query = String.format("SELECT COUNT(*) from task_rules");
            st = db.prepareStatement(query);
            resultSet = st.executeQuery();
            if (resultSet.next()) {
                count = resultSet.getInt("COUNT");
            }
            return count;
        } catch (Exception e) {
            LOGGER.error(String.format("Exception=PostgresException error while counting TaskRules."), e);
            throw new RuntimeException(e);
        } finally {
            closeResultStatementsConnections(resultSet, st, db);
        }
    }

    public List<TaskRule> getAllRules() {
        PreparedStatement st = null;
        ResultSet resultSet = null;
        Connection db = null;
        List<TaskRule> rules = new ArrayList<>();
        try {
            db = comboPooledDataSource.getConnection();
            String query = String.format("SELECT * from task_rules");
            st = db.prepareStatement(query);
            resultSet = st.executeQuery();

            while (resultSet.next()) {
                rules.add(convertToTaskRule(resultSet));
            }

        } catch (Exception e) {
            LOGGER.error(String.format("Exception=PostgresException error while getting TaskRules."), e);
            throw new RuntimeException(e);
        } finally {
            closeResultStatementsConnections(resultSet, st, db);
        }

        return rules;
    }

    public List<TaskRule> getRulesWithOffsetLimit(String clientId, int offset, int limit, String status, String taskBucket, String creatorId,
                                                  String modifierId) {
        PreparedStatement st = null;
        ResultSet resultSet = null;
        Connection db = null;
        List<TaskRule> rules = new ArrayList<>();
        try {
            db = comboPooledDataSource.getConnection();
            String query = String.format("SELECT * from task_rules " + getAndPredicate(clientId, creatorId, modifierId, status, taskBucket)
                                         + " ORDER BY updated_at DESC %s ? %s ?", Constants.OFFSET_STRING, Constants.LIMIT_STRING);

            st = db.prepareStatement(query);
            st.setInt(1, offset);
            st.setInt(2, limit);
            resultSet = st.executeQuery();

            while (resultSet.next()) {
                rules.add(convertToTaskRule(resultSet));
            }

        } catch (Exception e) {
            LOGGER.error(String.format("Exception=PostgresException error while getting TaskRules with offset and limit."), e);
            throw e;
        } finally {
            closeResultStatementsConnections(resultSet, st, db);
            return rules;
        }
    }

    private String getAndPredicate(String cliendId, String creatorId, String modifierId, String status, String taskBucket) {
        StringBuilder predicateBuilder = new StringBuilder(String.format("where client_id='%s'", cliendId.toLowerCase()));

        if (StringUtils.isNotBlank(creatorId)) {
            predicateBuilder.append(String.format(" AND creator_id='%s'", creatorId.toLowerCase()));
        }
        if (StringUtils.isNotBlank(modifierId)) {
            predicateBuilder.append(String.format(" AND last_updated_by='%s'", modifierId.toLowerCase()));
        }
        if (StringUtils.isNotBlank(status)) {
            predicateBuilder.append(String.format(" AND status='%s'", status.toUpperCase()));
        }
        if (StringUtils.isNotBlank(taskBucket)) {
            predicateBuilder.append(String.format(" AND task_bucket='%s'", taskBucket.toLowerCase()));
        }
        return predicateBuilder.toString();
    }

    public List<TaskRule> getRulesBySourceAndType(String source, String type) {
        PreparedStatement st = null;
        ResultSet resultSet = null;
        Connection db = null;
        List<TaskRule> rules = new ArrayList<>();
        try {
            db = comboPooledDataSource.getConnection();
            String query = String.format("SELECT * from task_rules where (%s,%s) = (?,?)", TaskRule.SOURCE, TaskRule.TYPE);
            st = db.prepareStatement(query);
            st.setString(1, source);
            st.setString(2, type);
            resultSet = st.executeQuery();

            while (resultSet.next()) {
                rules.add(convertToTaskRule(resultSet));
            }

        } catch (Exception e) {
            LOGGER.error(String.format("Exception=PostgresException error while getting TaskRules."), e);
            throw new RuntimeException(e);
        } finally {
            closeResultStatementsConnections(resultSet, st, db);
            return rules;
        }
    }

    public List<TaskRule> getRulesByAutoStart(Boolean autoStart) {
        PreparedStatement st = null;
        ResultSet resultSet = null;
        Connection db = null;
        List<TaskRule> rules = new ArrayList<>();
        try {
            db = comboPooledDataSource.getConnection();
            String query = String.format("SELECT * from task_rules where %s = ?", TaskRule.AUTO_START);
            st = db.prepareStatement(query);
            st.setBoolean(1, autoStart);
            resultSet = st.executeQuery();

            while (resultSet.next()) {
                rules.add(convertToTaskRule(resultSet));
            }

        } catch (Exception e) {
            LOGGER.error(String.format("Exception=PostgresException error while getting TaskRules."), e);
            throw new RuntimeException(e);
        } finally {
            closeResultStatementsConnections(resultSet, st, db);
            return rules;
        }
    }

    public List<TaskRule> getRulesByType(Set<String> typeSet) {
        return getAllRules().stream().
                filter(taskRule -> typeSet.contains(taskRule.getType())).
                                    collect(Collectors.toList());
    }

    public TaskRun getRun(int id) {
        PreparedStatement st = null;
        ResultSet resultSet = null;
        Connection db = null;
        try {
            db = comboPooledDataSource.getConnection();
            String query = String.format("SELECT * from task_runs where %s=?", TaskRun.ID);
            st = db.prepareStatement(query);
            st.setInt(1, id);
            resultSet = st.executeQuery();
            if (resultSet.next()) {
                return convertToTaskRun(resultSet);
            } else {
                LOGGER.info(String.format("Task run with id %s not found", id));
                return null;
            }
        } catch (Exception e) {
            LOGGER.error(String.format("Exception=PostgresException error while getting TaskRule with id %s", id), e);
            throw new RuntimeException(e);
        } finally {
            closeResultStatementsConnections(resultSet, st, db);
        }
    }

    /**
     * @param rule
     * @return TaskRule Id after update of the task rule
     */
    /*
    TODO: What to do when a finished task is updated, i.e that task with nextScheduleAt as null?
          ATM : We shall update that task and set its nextScheduleAt to something(currently keeping as currentTime)
            And set the status to Draft so that it can be run.
     */
    public TaskRule updateRule(TaskRule rule) {
        PreparedStatement st = null;
        ResultSet rs = null;
        Connection db = null;
        try {
            db = comboPooledDataSource.getConnection();
            String query = String.format("UPDATE task_rules SET (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                                         + "=(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) WHERE %s = ?", TaskRule.TYPE, TaskRule.STATUS, TaskRule.SEARCH_PARAMS,
                                         TaskRule.RECURRENCE, TaskRule.NEXT_SCHEDULE_AT, TaskRule.UPDATED_AT, TaskRule.SOURCE, TaskRule.LAST_UPDATED_BY,
                                         TaskRule.MAX_RECORDS, TaskRule.COUNTRY, TaskRule.AUTO_START, TaskRule.ID);

            long current = System.currentTimeMillis();
            st = db.prepareStatement(query);
            st.setString(1, rule.getType());
            st.setString(2, rule.getStatus().toString());
            st.setString(3, toJson(rule.getSearchParams()));
            st.setString(4, toJson(rule.getRecurrence()));
            st.setTimestamp(5, rule.getNextScheduleAt());
            st.setTimestamp(6, new Timestamp(current));
            st.setString(7, rule.getSource());
            st.setString(8, rule.getLastUpdatedBy());
            st.setInt(9, (rule.getMaxRecords() == null ? 0 : rule.getMaxRecords()));
            st.setString(10, rule.getCountry());
            st.setBoolean(11, rule.getAutoStart());
            st.setInt(12, rule.getId());
            st.executeUpdate();

            TaskRule updatedRule = getRule(rule.getId());

            if (updatedRule == null) {
                throw new RuntimeException("Exception=PostgresException something wrong with updating the TaskRule");
            }
            return updatedRule;

        } catch (Exception e) {
            LOGGER.error("Exception=PostgresException error while updating a TaskRule", e);
            throw new RuntimeException(e);
        } finally {
            closeResultStatementsConnections(rs, st, db);
        }
    }

    public void updateCountTaskRun(int count, int publishedCount, int runId) {
        PreparedStatement st = null;
        Connection db = null;
        try {
            db = comboPooledDataSource.getConnection();

            //update task_run
            String updateQuery = String.format("UPDATE task_runs SET %s = %s+?, %s=%s+? WHERE %s = ?",
                                               TaskRun.COUNT, TaskRun.COUNT, TaskRun.PUBLISHED_COUNT, TaskRun.PUBLISHED_COUNT, TaskRun.ID);
            st = db.prepareStatement(updateQuery);
            //set            
            st.setInt(1, count);
            st.setInt(2, publishedCount);
            //where
            st.setInt(3, runId);
            st.executeUpdate();

        } catch (SQLException e) {
            LOGGER.error("Exception=PostgresException error while updating task run", e);
        } finally {
            closeResultStatementsConnections(null, st, db);
        }
    }

    public void updateTaskRun(int runId) {
        PreparedStatement st = null;
        Connection db = null;
        try {
            db = comboPooledDataSource.getConnection();

            //update task_run
            String update_query = String.format("UPDATE task_runs SET (%s) =  (?) WHERE %s = ?",
                                                TaskRun.UPDATED_AT, TaskRun.ID);
            long current = System.currentTimeMillis();
            Timestamp currentTs = new Timestamp(current);
            st = db.prepareStatement(update_query);
            //set
            st.setTimestamp(1, currentTs);
            //where
            st.setInt(2, runId);
            st.executeUpdate();

        } catch (Exception e) {
            LOGGER.error("Exception=PostgresException error while updating task run", e);
        } finally {
            closeResultStatementsConnections(null, st, db);
        }
    }

    public TaskRun createTaskRun(TaskRule rule) {
        PreparedStatement st_task_run = null;
        Connection db = null;
        ResultSet resultSet = null;
        TaskRun taskRun = null;

        try {
            db = comboPooledDataSource.getConnection();
            long current = System.currentTimeMillis();
            Timestamp currentTs = new Timestamp(current);

            //update task_run
            String query_task_run = String.format("INSERT INTO task_runs (%s, %s, %s, %s, %s) "
                                                  + "VALUES (?, ?, ?, ?, ?)", TaskRun.TASK_RULE_ID, TaskRun.CREATED_AT, TaskRun.UPDATED_AT, TaskRun.COUNT,
                                                  TaskRun.PUBLISHED_COUNT);
            st_task_run = db.prepareStatement(query_task_run, Statement.RETURN_GENERATED_KEYS);
            st_task_run.setInt(1, rule.getId());
            st_task_run.setTimestamp(2, currentTs);
            st_task_run.setTimestamp(3, currentTs);
            st_task_run.setInt(4, 0);
            st_task_run.setInt(5, 0);
            st_task_run.executeUpdate();

            resultSet = st_task_run.getGeneratedKeys();

            if (resultSet.next()) {
                int id = resultSet.getInt(TaskRun.ID);
                taskRun = getRun(id);
            }


        } catch (Exception e) {
            LOGGER.error("Exception=PostgresException error while updating task run", e);
        } finally {
            closeResultStatementsConnections(null, st_task_run, db);
        }

        if (taskRun == null) {
            throw new RuntimeException("Exception=PostgresException something wrong with adding a new TaskRun, "
                                       + "the task run was added but unable to fetch its id after insert");
        }

        return taskRun;
    }

    public void markTaskRuleFinished(int ruleId) {
        PreparedStatement st = null;
        Connection db = null;
        try {
            db = comboPooledDataSource.getConnection();

            //update task_rule
            String update_query = String.format("UPDATE task_rules SET (%s) = (?),(%s) = (?) WHERE (%s) = (?)",
                                                TaskRule.STATUS, TaskRule.UPDATED_AT, TaskRule.ID);
            long current = System.currentTimeMillis();
            Timestamp currentTs = new Timestamp(current);
            st = db.prepareStatement(update_query);
            //set
            st.setString(1, TaskRule.Status.FINISHED.toString());
            st.setTimestamp(2, currentTs);
            //where
            st.setInt(3, ruleId);
            int row_updated = st.executeUpdate();
            if (row_updated != 0) {
                LOGGER.info(String.format("Event=PostgresUpdate: Completed rule_id=%d, marked status=FINISHED.", ruleId));
            }

        } catch (Exception e) {
            LOGGER.error("Exception=PostgresException error while updating task rule status", e);
        } finally {
            closeResultStatementsConnections(null, st, db);
        }
    }

    public void updateTaskRuleStatus(int ruleId, TaskRule.Status ruleStatus) {
        PreparedStatement st = null;
        Connection db = null;
        try {
            db = comboPooledDataSource.getConnection();

            //update task_rule
            String update_query = String.format("UPDATE task_rules SET (%s) = (?),(%s) = (?) WHERE (%s) = (?)",
                                                TaskRule.STATUS, TaskRule.UPDATED_AT, TaskRule.ID);
            long current = System.currentTimeMillis();
            Timestamp currentTs = new Timestamp(current);
            st = db.prepareStatement(update_query);
            //set
            st.setString(1, ruleStatus.toString());
            st.setTimestamp(2, currentTs);
            //where
            st.setInt(3, ruleId);
            int row_updated = st.executeUpdate();
            if (row_updated != 0) {
                LOGGER.info(String.format("Event=PostgresUpdate: Completed rule_id=%d, marked status=%s.", ruleId, ruleStatus));
            }

        } catch (Exception e) {
            LOGGER.error("Exception=PostgresException error while updating task rule status", e);
        } finally {
            closeResultStatementsConnections(null, st, db);
        }
    }

    public void updateTaskRunStatus(int runId, String status) {
        PreparedStatement st = null;
        Connection db = null;
        try {
            db = comboPooledDataSource.getConnection();

            //update task_rule
            String update_query = String.format("UPDATE task_runs SET (%s) = (?),(%s) = (?) WHERE (%s) = (?)",
                                                TaskRun.STATUS, TaskRule.UPDATED_AT, TaskRun.ID);
            long current = System.currentTimeMillis();
            Timestamp currentTs = new Timestamp(current);
            st = db.prepareStatement(update_query);
            //set
            st.setString(1, status);
            st.setTimestamp(2, currentTs);
            //where
            st.setInt(3, runId);
            int row_updated = st.executeUpdate();
            if (row_updated != 0) {
                LOGGER.info(String.format("Event=PostgresUpdate: Updated run_id=%d, marked status=%s.", runId, status));
            }

        } catch (Exception e) {
            LOGGER.error("Exception=PostgresException error while updating task run status", e);
        } finally {
            closeResultStatementsConnections(null, st, db);
        }
    }

    private void closeStatement(PreparedStatement st) {
        if (st != null) {
            try {
                st.close();
            } catch (SQLException e) {
                LOGGER.error("Exception=PostgresException error while closing statement", e);
            }
        }
    }

    private void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                LOGGER.error("Exception=PostgresException error while closing connection", e);
            }
        }
    }

    private void closeResults(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                LOGGER.error("Exception=PostgresException error while closing result-set", e);
            }
        }
    }

    private Timestamp getDef(Timestamp value, Timestamp def) {
        if (value == null) {
            return def;
        } else {
            return value;
        }
    }

    private TaskRule convertToTaskRule(ResultSet resultSet) throws SQLException, IOException {
        if (resultSet != null) {
            TaskRule.Builder builder = new TaskRule.Builder();
            builder.id(resultSet.getInt(TaskRule.ID));
            builder.source(resultSet.getString(TaskRule.SOURCE));
            builder.lastUpdatedBy(resultSet.getString(TaskRule.LAST_UPDATED_BY));
            builder.type(resultSet.getString(TaskRule.TYPE));
            builder.maxRecords(resultSet.getInt(TaskRule.MAX_RECORDS));
            builder.status(TaskRule.Status.fromString(resultSet.getString(TaskRule.STATUS)));
            builder.searchParamsMap(fromJson(resultSet.getString(TaskRule.SEARCH_PARAMS)));
            builder.recurrence(recurrenceFromJson(resultSet.getString(TaskRule.RECURRENCE)));
            builder.nextScheduleAt(resultSet.getTimestamp(TaskRule.NEXT_SCHEDULE_AT));
            builder.createdAt(resultSet.getTimestamp(TaskRule.CREATED_AT));
            builder.updatedAt(resultSet.getTimestamp(TaskRule.UPDATED_AT));
            builder.creatorId(resultSet.getString(TaskRule.CREATOR_ID));
            builder.clientId(resultSet.getString(TaskRule.CLIENT_ID));
            builder.taskBucket(resultSet.getString(TaskRule.TASK_BUCKET));
            builder.country(resultSet.getString(TaskRule.COUNTRY));
            builder.autoStart(resultSet.getBoolean(TaskRule.AUTO_START));
            return builder.build();
        }

        throw new IllegalArgumentException("could not convert result-set to TaskRule as result-set passed is null");
    }

    private TaskBatchRequest convertToTaskBatchRequest(ResultSet resultSet) throws SQLException {
        if (resultSet != null) {
            TaskBatchRequest.Builder builder = new TaskBatchRequest.Builder();
            builder.id(resultSet.getInt(TaskBatchRequest.ID));
            builder.source(resultSet.getString(TaskBatchRequest.SOURCE));
            builder.createdAt(resultSet.getTimestamp(TaskBatchRequest.CREATED_AT));
            builder.updatedAt(resultSet.getTimestamp(TaskBatchRequest.UPDATED_AT));
            builder.fetchRequestJsonString(resultSet.getString(TaskBatchRequest.FETCH_REQUEST));
            builder.status(resultSet.getString(TaskBatchRequest.STATUS));
            return builder.build();
        }
        throw new IllegalArgumentException("could not convert result-set to TaskRule as result-set passed is null");
    }

    private TaskRun convertToTaskRun(ResultSet resultSet) throws SQLException {

        if (resultSet != null) {
            TaskRun.Builder builder = new TaskRun.Builder();
            builder.id(resultSet.getInt(TaskRun.ID));
            builder.taskRuleId(resultSet.getInt(TaskRun.TASK_RULE_ID));
            builder.count(resultSet.getInt(TaskRun.COUNT));
            builder.published_count(resultSet.getInt(TaskRun.PUBLISHED_COUNT));
            builder.stateSnapshot(resultSet.getString(TaskRun.STATE_SNAPSHOT));
            builder.createdAt(resultSet.getTimestamp(TaskRun.CREATED_AT));
            builder.updatedAt(resultSet.getTimestamp(TaskRun.UPDATED_AT));
            builder.status(resultSet.getString(TaskRun.STATUS));
            return builder.build();
        }

        throw new IllegalArgumentException("could not convert result-set to TaskRule as result-set passed is null");

    }

    private String toJson(SearchParams paramMap) throws JsonProcessingException {
        if (paramMap == null) {
            return null;
        }
        return JSON_MAPPER.writeValueAsString(paramMap);
    }

    private String toJson(RuleRecurrence recurrence) throws JsonProcessingException {
        if (recurrence == null) {
            return null;
        }
        return JSON_MAPPER.writeValueAsString(recurrence);
    }

    private SearchParams fromJson(String json) throws JsonParseException, JsonMappingException, IOException {
        if (json == null) {
            return null;
        }
        return JSON_MAPPER.readValue(json, SearchParams.class);
    }

    private RuleRecurrence recurrenceFromJson(String json) throws JsonParseException, JsonMappingException, IOException {
        if (json == null) {
            return null;
        }
        return JSON_MAPPER.readValue(json, RuleRecurrence.class);
    }

    public void closeResultStatementsConnections(ResultSet resultSet, PreparedStatement preparedStatement, Connection connection) {
        closeResults(resultSet);
        closeStatement(preparedStatement);
        closeConnection(connection);
    }

    public void saveUrlsFetchedCount(Integer ruleId, int urlsFetched) {
        PreparedStatement st = null;
        Connection db = null;
        try {
            db = comboPooledDataSource.getConnection();

            //update task_rule table
            String update_query = String.format("UPDATE task_rules SET (%s, %s) = (?,?) WHERE %s = ?",
                                                Constants.URLS_FETCHED_COUNT, TaskRule.UPDATED_AT, TaskRule.ID);
            long current = System.currentTimeMillis();
            Timestamp currentTs = new Timestamp(current);
            st = db.prepareStatement(update_query);
            //set
            st.setInt(1, urlsFetched);
            st.setTimestamp(2, currentTs);
            //where
            st.setInt(3, ruleId);
            st.executeUpdate();

        } catch (Exception e) {
            LOGGER.error(String.format("Exception=PostgresException error while saving urlsFetchedCount for ruleId=%d, exception=%s",
                                       ruleId, e));
        } finally {
            closeResultStatementsConnections(null, st, db);
        }
    }

    public int getUrlsFetchedCount(int ruleId) {
        PreparedStatement st = null;
        Connection db = null;
        ResultSet resultSet = null;
        int urlsFetched = 0;
        try {
            db = comboPooledDataSource.getConnection();
            //Read urlsFetched from task_rules
            String update_query = String.format("SELECT %s FROM task_rules WHERE %s = ?",
                                                Constants.URLS_FETCHED_COUNT, TaskRule.ID);
            st = db.prepareStatement(update_query);
            //where
            st.setInt(1, ruleId);
            resultSet = st.executeQuery();
            if (resultSet.next()) {
                urlsFetched = resultSet.getInt(Constants.URLS_FETCHED_COUNT);
            }
            return urlsFetched;

        } catch (Exception e) {
            LOGGER.error(String.format("Exception=PostgresException error while reading urlsFetchedCount for ruleId=%d, exception=%s",
                                       ruleId, e));
            throw new RuntimeException(e);
        } finally {
            closeResultStatementsConnections(null, st, db);
        }
    }
}
