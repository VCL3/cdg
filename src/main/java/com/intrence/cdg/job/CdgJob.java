package com.intrence.cdg.job;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.intrence.config.ConfigProvider;
import com.intrence.config.collection.ConfigMap;
import com.intrence.core.modules.PostgresModule;
import com.intrence.core.persistence.dao.ProductDao;
import com.intrence.core.persistence.postgres.PostgresConfig;
import com.intrence.models.model.Product;

import java.util.UUID;

/**
 * Created by wliu on 12/1/17.
 */
public class CdgJob {

    public static void main(String[] args) throws Exception {

        Injector injector = Guice.createInjector(new PostgresModule());

        PostgresConfig postgresConfig = injector.getInstance(PostgresConfig.class);

        ProductDao productDao = injector.getInstance(ProductDao.class);

        Product product = productDao.getProductById(UUID.fromString("81ae3b00-81b4-4cbd-9662-1e5db30a1f7d"));

        System.out.println(product.toJson());

    }
}
