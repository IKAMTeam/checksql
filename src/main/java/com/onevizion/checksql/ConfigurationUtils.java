package com.onevizion.checksql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.filter.Filters;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;

import com.onevizion.checksql.vo.Configuration;

public class ConfigurationUtils {

    public static Configuration loadConfiguration(Document doc) {
        return loadConfigurationFromFile(doc);
    }

    private static Configuration loadConfigurationFromFile(Document doc) {
        Configuration configuration = new Configuration();
        List<String> skipTablesSqlList = new ArrayList<String>();
        List<String> skipTablesPlSqlList = new ArrayList<String>();

        XPathExpression<Element> xpath = XPathFactory.instance().compile("root", Filters.element());
        for (Element elem : xpath.evaluate(doc)) {
            configuration.setRemoteOwner(elem.getChildText("remote_owner"));
            configuration.setRemoteUser(elem.getChildText("remote_user"));
            configuration.setLocalOwner(elem.getChildText("local_owner"));
            configuration.setLocalUser(elem.getChildText("local_user"));
            configuration.setEnabledSql(Boolean.parseBoolean(elem.getChild("sql").getChildText("enabled")));
            configuration.setEnabledPlSql(Boolean.parseBoolean(elem.getChild("pl_sql").getChildText("enabled")));

            String skipTablesSql = elem.getChild("sql").getChildText("disable-tables");
            if (StringUtils.isNotBlank(skipTablesSql)) {
                skipTablesSqlList = Arrays.asList(skipTablesSql.split(","));
            }
            configuration.setSkipTablesSql(skipTablesSqlList);

            String skipTablesPlSql = elem.getChild("pl_sql").getChildText("disable-tables");
            if (StringUtils.isNotBlank(skipTablesPlSql)) {
                skipTablesPlSqlList = Arrays.asList(skipTablesPlSql.split(","));
            }
            configuration.setSkipTablesPlSql(skipTablesPlSqlList);

            if (StringUtils.isNotBlank(configuration.getLocalOwner()) && StringUtils.isNotBlank(configuration.getLocalUser())) {
                configuration.setUseSecondTest(true);
            }
        }

        return configuration;
    }

}