package com.intrence.cdg.frontier;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.intrence.cdg.util.Constants;
import com.intrence.config.ConfigProvider;
import com.intrence.config.collection.ConfigMap;
import com.intrence.cdg.net.FetchRequest;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;
import redis.clients.jedis.*;
import redis.clients.util.Pool;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Component
public class RedisFetchReqFrontier implements FetchRequestFrontier {

    private static final Logger LOGGER = Logger.getLogger(RedisFetchReqFrontier.class);
    private static final ConfigMap CONFIG_MAP = ConfigProvider.getConfig();
    private static final String CACHE_PREFIX = "cache-";

    Pool<Jedis> pool;
    Set<HashFunction> hashFns = initHashes();

    private static Set<HashFunction> initHashes() {
        Set<HashFunction> hashFns = new HashSet<>();
        hashFns.add(Hashing.adler32());
        hashFns.add(Hashing.crc32());
        hashFns.add(Hashing.murmur3_32());
        hashFns.add(Hashing.crc32c());
        return hashFns;
    }

    @PostConstruct
    public void init() {
        if (Constants.DEV_ENVIRONMENTS.contains(ConfigProvider.getEnvironment())) {
//            String masterName = CONFIG_MAP.getMap("redis").getString("masterName");
//            String sentinelPoolStr = CONFIG_MAP.getMap("redis").getString("sentinelPool");
//            String[] poolHosts = sentinelPoolStr.split(",");
//            Set<String> sentinels = new HashSet<>();
//            for (String host : poolHosts) {
//                sentinels.add(host.trim());
//            }
//            pool = new JedisSentinelPool(masterName.trim(), sentinels, new GenericObjectPoolConfig(), 10000, null, Protocol.DEFAULT_DATABASE);

            pool = new JedisPool("127.0.0.1", 6379);

        } else {
            String redisHosts = CONFIG_MAP.getMap("redis").getString("host");
            String host = redisHosts.split(":")[0];
            int port = Integer.valueOf(redisHosts.split(":")[1]);
            pool = new JedisPool(host, port);
        }

    }

    @PreDestroy
    public void shutdown() {
        pool.destroy();
    }

    @Override
    public FetchRequest getNext(int taskRuleId) {

        Jedis jedis = null;
        FetchRequest fetchReq = null;
        long start = System.currentTimeMillis();
        try {
            jedis = pool.getResource();
            Set<Tuple> set = jedis.zrevrangeWithScores(String.valueOf(taskRuleId), 0, 0);
            for (Tuple tuple : set) {
                fetchReq = FetchRequest.fromJson(tuple.getElement());
                break;
            }
            return fetchReq;
        } finally {
            close(jedis);
            logTime("RedisGetNext", taskRuleId, start);
        }
    }

    @Override
    public void delete(int taskRuleId, FetchRequest fetchReq) {
        Jedis jedis = null;
        long start = System.currentTimeMillis();
        try {
            jedis = pool.getResource();
            jedis.zrem(String.valueOf(taskRuleId), fetchReq.toJson());
        } catch (JsonProcessingException e) {
            //json processing exception
            LOGGER.warn(String.format("Exception=%s when deleting fetchRequest=%s to redis for taskrule=%s",
                                      e.getMessage(),
                                      fetchReq.getWorkRequest(),
                                      taskRuleId));
        } finally {
            close(jedis);
            logTime("RedisDelete", taskRuleId, start);
        }
    }

    @Override
    public Set<FetchRequest> getTopK(int taskRuleId, int K) {
        Set<FetchRequest> topK = new HashSet<>();
        Jedis jedis = null;
        long start = System.currentTimeMillis();
        try {
            jedis = pool.getResource();
            LOGGER.info("jedis connection " + jedis.info());
            if (K > 0) {
                Set<Tuple> set = jedis.zrevrangeWithScores(String.valueOf(taskRuleId), new Long(0), new Long(K - 1));
                for (Tuple tuple : set) {
                    topK.add(FetchRequest.fromJson(tuple.getElement()));
                }
            }
            return topK;
        } finally {
            close(jedis);
            logTime("RedisGetTopK", taskRuleId, start);
        }
    }

    @Override
    public void add(int taskRuleId, FetchRequest fetchReq) {
        Jedis jedis = null;
        long start = System.currentTimeMillis();
        String fetchRequestJson;
        try {
            jedis = pool.getResource();
            if (!isSeen(jedis, taskRuleId, fetchReq)) {
                fetchRequestJson = fetchReq.toJson();
                jedis.zadd(String.valueOf(taskRuleId),
                           fetchReq.getPriority(), fetchRequestJson);
                addToCache(jedis, taskRuleId, fetchRequestJson);
            }
        }
        catch (JsonProcessingException e) {
            //json processing exception
            LOGGER.warn(String.format("Exception=%s when adding fetchRequest=%s to redis for taskrule=%s",
                                      e.getMessage(),
                                      fetchReq.getWorkRequest(),
                                      taskRuleId));
        }
        finally {
            close(jedis);
            logTime("RedisAdd", taskRuleId, start);
        }
    }


    // redis has limits on number of entries that could be added to set in single shot
    //  - so, adding in batches
    @Override
    public void addAll(int taskRuleId, Set<FetchRequest> fetchReqs) {
        int maxInABatch = 1000;
        int count = 0;
        Set<FetchRequest> curBatch = new HashSet<>();
        for (FetchRequest req : fetchReqs) {
            if (req != null) {
                curBatch.add(req);
                count++;
                if (count >= maxInABatch) {
                    addAllInternal(taskRuleId, curBatch);
                    count = 0;
                    curBatch = new HashSet<>();
                }
            }
        }
        if (!curBatch.isEmpty()) {
            addAllInternal(taskRuleId, curBatch);
        }
    }

    @Override
    public void deleteAll(int taskRuleId, Set<FetchRequest> fetchReqs) {

        if (CollectionUtils.isEmpty(fetchReqs)) {
            return;
        }

        Jedis jedis = null;
        long start = System.currentTimeMillis();
        try {
            jedis = pool.getResource();
            String[] members = new String[fetchReqs.size()];
            int i = 0;
            for (FetchRequest fetchReq : fetchReqs) {
                members[i] = fetchReq.toJson();
                i++;
            }
            jedis.zrem(String.valueOf(taskRuleId), members);
        }
        catch (JsonProcessingException e) {
            //json processing exception
            LOGGER.warn(String.format(
                    "Exception=%s when deleting set of fetchRequests(size=%s) to redis for taskrule=%s",
                    e.getMessage(),
                    fetchReqs.size(),
                    taskRuleId));
        }
        finally {
            close(jedis);
            logTime("RedisDeleteAll", taskRuleId, start);
        }
    }

    @Override
    public void deleteKey(int taskRuleId) {
        Jedis jedis = null;
        long start = System.currentTimeMillis();
        try {
            jedis = pool.getResource();
            jedis.del(String.valueOf(taskRuleId), CACHE_PREFIX + taskRuleId);
        }
        finally {
            close(jedis);
            logTime("RedisDeleteKey", taskRuleId, start);
        }
    }

    //currently only used in test
    public void addToCache(int taskRuleId, String value) {
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            addToCache(jedis, taskRuleId, value);
        }
        finally {
            close(jedis);
        }
    }

    public boolean isSeen(int taskRuleId, FetchRequest fetchReq) {
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            return isSeen(jedis, taskRuleId, fetchReq);
        }
        catch (JsonProcessingException e) {
            //json processing exception
            LOGGER.warn(String.format("Exception=%s when checking isSeen for fetchRequest=%s to redis for taskrule=%s",
                                      e.getMessage(),
                                      fetchReq.getWorkRequest(),
                                      taskRuleId));
            return false;
        }
        finally {
            close(jedis);
        }
    }

    protected Set<Long> hash(String s) {
        Set<Long> hashes = new HashSet<>();
        for (HashFunction hashFn : hashFns) {
            HashCode hashCode = hashFn.hashUnencodedChars(s);
            int h = hashCode.asInt();
            if (h < 0) {
                int positiveH = Math.abs(h);
                hashes.add((long) Integer.MAX_VALUE + (long) positiveH);
            }
            else {
                hashes.add((long) h);
            }
        }
        return hashes;
    }

    protected boolean isSeen(Jedis jedis, int taskRuleId, FetchRequest fetchReq) throws JsonProcessingException {
        return _isSeen(jedis, (CACHE_PREFIX + taskRuleId), fetchReq.toJson());
    }

    protected boolean _isSeen(Jedis jedis, String key, String value) {
        Set<Long> hashes = hash(value);
        boolean seen = true;
        for (long hash : hashes) {
            seen = seen & jedis.getbit(key, hash);
        }
        return seen;
    }

    protected void close(Jedis jedis) {
        if (jedis != null) {
            jedis.close();
        }
    }

    protected void logTime(String event, int key, long start) {
        LOGGER.info(String.format("Event=%s key=%s timeTaken=%s", event, key, (System.currentTimeMillis() - start)));
    }

    protected void logTime(String event, String key, long start) {
        LOGGER.info(String.format("Event=%s key=%s timeTaken=%s", event, key, (System.currentTimeMillis() - start)));
    }

    protected void addToCache(Jedis jedis, int taskRuleId, String value) {
        _addToCache(jedis, (CACHE_PREFIX + taskRuleId), value);
    }

    protected void _addToCache(Jedis jedis, String key, String value) {
        Set<Long> hashes = hash(value);
        for (long hash : hashes) {
            jedis.setbit(key, hash, true);
        }
    }

    private void addAllInternal(int taskRuleId, Set<FetchRequest> fetchReqs) {
        Jedis jedis = null;
        long start = System.currentTimeMillis();
        try {
            jedis = pool.getResource();

            Map<String, Double> scoredMembers = new HashMap<>();
            for (FetchRequest fetchReq : fetchReqs) {
                if (!isSeen(jedis, taskRuleId, fetchReq)) {
                    scoredMembers.put(fetchReq.toJson(), (double) fetchReq.getPriority());
                }
            }
            if (!scoredMembers.isEmpty()) {
                jedis.zadd(String.valueOf(taskRuleId), scoredMembers);
                for (String member : scoredMembers.keySet()) {
                    addToCache(jedis, taskRuleId, member);
                }
            }
        }
        catch (JsonProcessingException e) {
            //json processing exception
            LOGGER.warn(String.format("Exception=%s when adding set of fetchRequests(size=%s) to redis for taskrule=%s",
                                      e.getMessage(),
                                      fetchReqs.size(),
                                      taskRuleId));
        }
        finally {
            close(jedis);
            logTime("RedisAddAll", taskRuleId, start);
        }
    }
}
