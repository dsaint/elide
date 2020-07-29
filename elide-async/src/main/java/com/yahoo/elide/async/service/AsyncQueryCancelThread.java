/*
 * Copyright 2020, Yahoo Inc.
 * Licensed under the Apache License, Version 2.0
 * See LICENSE file in project root for terms.
 */
package com.yahoo.elide.async.service;

import static com.yahoo.elide.core.EntityDictionary.NO_VERSION;

import com.yahoo.elide.Elide;
import com.yahoo.elide.async.models.AsyncQuery;
import com.yahoo.elide.async.models.QueryStatus;
import com.yahoo.elide.core.DataStoreTransaction;
import com.yahoo.elide.core.EntityDictionary;
import com.yahoo.elide.core.RequestScope;
import com.yahoo.elide.core.TransactionRegistry;
import com.yahoo.elide.core.filter.dialect.RSQLFilterDialect;
import com.yahoo.elide.core.filter.expression.FilterExpression;
import com.yahoo.elide.jsonapi.models.JsonApiDocument;
import com.google.common.collect.Sets;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

/**
 * Runnable thread for cancelling AsyncQuery transactions
 * beyond the max run time or if it has status CANCELLED.
 */
@Slf4j
@Data
@AllArgsConstructor
public class AsyncQueryCancelThread implements Runnable {

    private int maxRunTimeSeconds;
    private Elide elide;
    private AsyncQueryDAO asyncQueryDao;
    private EntityDictionary dictionary;
    private RSQLFilterDialect filterParser;

    @Override
    public void run() {
        cancelAsyncQuery();
    }

    /**
     * This method cancels queries based on threshold.
     */
    protected void cancelAsyncQuery() {

        try {

            TransactionRegistry transactionRegistry = elide.getTransactionRegistry();
            Map<UUID, DataStoreTransaction> runningTransactionMap = transactionRegistry.getRunningTransactions();

            String filterExpression = "status=in=(" + QueryStatus.CANCELLED.toString() + ","
                    + QueryStatus.PROCESSING.toString() + ","
                    + QueryStatus.QUEUED.toString() + ")";
            FilterExpression filter = filterParser.parseFilterExpression(filterExpression,
                    AsyncQuery.class, false);
            Collection<AsyncQuery> asyncQueryCollection = asyncQueryDao.getActiveAsyncQueryCollection(filter);

            Set<UUID> runningTransactions = runningTransactionMap.keySet();

            Set<UUID> asyncTransactions = asyncQueryCollection.stream()
                    .filter(query -> query.getStatus() == QueryStatus.CANCELLED
                    || TimeUnit.SECONDS.convert(Math.abs(query.getUpdatedOn().getTime()
                    - new Date(System.currentTimeMillis()).getTime()), TimeUnit.MILLISECONDS) > maxRunTimeSeconds)
                    .map(AsyncQuery::getRequestId)
            .collect(Collectors.toSet());

            Set<UUID> queriesToCancel = Sets.intersection(runningTransactions, asyncTransactions);

            queriesToCancel.stream()
               .forEach((tx) -> {
                   JsonApiDocument jsonApiDoc = new JsonApiDocument();
                   MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
                   RequestScope scope = new RequestScope("query", NO_VERSION, jsonApiDoc,
                           transactionRegistry.getRunningTransaction(tx), null, queryParams,
                           tx, elide.getElideSettings());
                   transactionRegistry.getRunningTransaction(tx).cancel(scope);
               });

        } catch (Exception e) {
            log.error("Exception: {}", e);
        }
    }
}
