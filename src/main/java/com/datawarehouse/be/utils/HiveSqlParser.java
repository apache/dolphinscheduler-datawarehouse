/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datawarehouse.be.utils;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;

import java.util.List;
import java.util.Optional;

/**
 * Hive Sql Parser
 */
public class HiveSqlParser {
    /**
     * 输入表信息
     */

    /**
     * 输出表信息
     */
    private List<OutputTable> outputTableList = Lists.newArrayList();
    /**
     * 表信息
     */
    private List<Table> tableList = Lists.newArrayList();
    /**
     * 临时表信息
     */
    private List<Table> tempTableList = Lists.newArrayList();


    public static void main(String[] args) {
        String sql = "INSERT OVERWRITE TABLE test.bz_user partition(dt='20210801') select * from test.dep where dt='20210801' and dt = '$[yyyyMMdd-1]' and dt = '$[20210801]'";

        ParseDriver pd = new ParseDriver();
        try {
            ASTNode tree = pd.parse(sql);
            System.out.println(tree.dump());
            System.out.println(tree.toStringTree());
            HiveSqlParser hiveSqlParser = new HiveSqlParser();
            hiveSqlParser.sqlParser(sql);
            System.out.println(JSON.toJSONString(hiveSqlParser.outputTableList));
            System.out.println(JSON.toJSONString(hiveSqlParser.tableList));
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public void sqlParser(String sql) throws ParseException {
        if (StringUtils.isBlank(sql)) {
            return;
        }

        ParseDriver parseDriver = new ParseDriver();
        ASTNode tree = parseDriver.parse(sql);

        // 递归树
        recursionTree(tree, sql);
    }

    private void recursionTree(ASTNode tree, String sql) {
        List<Node> treeChildren = tree.getChildren();

        if (treeChildren == null || CollectionUtils.isEmpty(treeChildren)) {
            return;
        }

        for (Node treeChild : treeChildren) {
            // 只处理抽象语法树节点
            if (treeChild instanceof ASTNode) {
                ASTNode treeChildAstNode = (ASTNode) treeChild;
                // 获取节点token类型
                Integer nodeType = treeChildAstNode.getType();
                // 忽略token类型为null节点（根节点）
                if (nodeType == null) {
                    return;
                }

                // 忽略EOF
                if (HiveParser.EOF == nodeType.intValue()) {
                    return;
                }
                // 输出表分区
                if (HiveParser.TOK_PARTVAL == nodeType.intValue()) {
                    wrapOutputTable(treeChildAstNode);
                }
                // 修改、删除表
                else if (HiveParser.TOK_UPDATE_TABLE == nodeType.intValue()
                        || HiveParser.TOK_DELETE_FROM == nodeType.intValue()
                        || HiveParser.TOK_TABREF == nodeType.intValue()
                        || HiveParser.TOK_CREATETABLE == nodeType.intValue()
                        || HiveParser.TOK_DROPTABLE == nodeType.intValue()) {
                    wrapTable(treeChildAstNode, OperationType.getOperationType(nodeType.intValue()), sql);
                } else if (HiveParser.TOK_INSERT == nodeType.intValue()) {
                    wrapInsertTable(treeChildAstNode, OperationType.getOperationType(nodeType.intValue()), sql);
                } else if (HiveParser.TOK_CTE == nodeType.intValue()) {
                    // 临时表
                    wrapTempTable(treeChildAstNode, OperationType.getOperationType(nodeType.intValue()), sql);
                }

                // 继续递归
                recursionTree(treeChildAstNode, sql);
            }
        }
    }

    private void wrapTempTable(ASTNode treeChildAstNode, OperationType operationType, String sql) {
        List<Node> treeChildrenList = treeChildAstNode.getChildren();
        if (CollectionUtils.isEmpty(treeChildrenList)) {
            return;
        }

        for (Node node : treeChildrenList) {
            if (node instanceof ASTNode) {
                ASTNode astNode = (ASTNode) node;
                // 获取节点token类型
                Integer nodeType = astNode.getType();
                if (nodeType == HiveParser.TOK_SUBQUERY) {
                    List<Node> subQueryNodes = astNode.getChildren();
                    if (CollectionUtils.isEmpty(subQueryNodes)) {
                        return;
                    }
                    for (Node subQuery : subQueryNodes) {
                        if (subQuery instanceof ASTNode) {
                            ASTNode subQueryAstNode = (ASTNode) subQuery;
                            if (subQueryAstNode.getType() == HiveParser.Identifier) {
                                this.tempTableList.add(new Table(null, subQueryAstNode.getText(), operationType, sql));
                            }
                        }
                    }
                }

            }
        }
    }

    private void wrapInsertTable(ASTNode treeChildAstNode, OperationType operationType, String sql) {
        List<Node> treeChildrenList = treeChildAstNode.getChildren();
        if (CollectionUtils.isEmpty(treeChildrenList)) {
            return;
        }

        for (Node node : treeChildrenList) {
            if (node instanceof ASTNode) {
                ASTNode astNode = (ASTNode) node;
                // 获取节点token类型
                Integer nodeType = astNode.getType();
                if (nodeType == HiveParser.TOK_DESTINATION) {
                    List<Node> destinationNodes = astNode.getChildren();
                    if (CollectionUtils.isEmpty(destinationNodes)) {
                        return;
                    }
                    for (Node destination : destinationNodes) {
                        if (destination instanceof ASTNode) {
                            ASTNode destinationAstNode = (ASTNode) destination;

                            if (destinationAstNode.getType() == HiveParser.TOK_TAB) {
                                List<Node> tabNodeList = destinationAstNode.getChildren();
                                if (CollectionUtils.isEmpty(tabNodeList)) {
                                    return;
                                }
                                wrapTable(destinationAstNode, operationType, sql);
                            }
                        }
                    }
                }
            }

        }
    }

    private void wrapTable(ASTNode treeChildAstNode, OperationType operationType, String sql) {
        List<Node> treeChildrenList = treeChildAstNode.getChildren();
        if (CollectionUtils.isEmpty(treeChildrenList)) {
            return;
        }

        for (Node node : treeChildrenList) {
            if (node instanceof ASTNode) {
                ASTNode astNode = (ASTNode) node;
                // 获取节点token类型
                Integer nodeType = astNode.getType();
                if (nodeType == HiveParser.TOK_TABNAME) {
                    List<Node> tableNameNodes = astNode.getChildren();
                    if (CollectionUtils.isEmpty(tableNameNodes)) {
                        return;
                    }
                    String dbName = null;
                    String tableName = null;

                    if (tableNameNodes.size() < 2) {
                        // 未包含库名
                        Node tableNameNode = tableNameNodes.get(0);
                        if (tableNameNode instanceof ASTNode) {
                            tableName = ((ASTNode) tableNameNode).getText();
                        }
                    } else {
                        // 包含库名.表名
                        Node dbNameNode = tableNameNodes.get(0);
                        if (dbNameNode instanceof ASTNode) {
                            dbName = ((ASTNode) dbNameNode).getText();
                        }
                        Node tableNameNode = tableNameNodes.get(1);
                        if (tableNameNode instanceof ASTNode) {
                            tableName = ((ASTNode) tableNameNode).getText();
                        }
                    }
                    this.tableList.add(new Table(dbName, tableName, operationType, sql));
                }
            }

        }
    }

    private void wrapOutputTable(ASTNode treeChildAstNode) {
        List<Node> tableNameNodes = treeChildAstNode.getChildren();
        if (CollectionUtils.isEmpty(tableNameNodes) || tableNameNodes.size() != 2) {
            return;
        }
        String partitionName = null;
        String partitionValue = null;

        Node dbNameNode = tableNameNodes.get(0);
        if (dbNameNode instanceof ASTNode) {
            partitionName = ((ASTNode) dbNameNode).getText();
        }
        Node tableNameNode = tableNameNodes.get(1);
        if (tableNameNode instanceof ASTNode) {
            partitionValue = ((ASTNode) tableNameNode).getText();
        }
        this.outputTableList.add(new OutputTable(partitionName, partitionValue));
    }

    private Integer getTokenType(ASTNode treeChildAstNode) {
        return Optional.ofNullable(treeChildAstNode)
                .map(e -> e.getToken())
                .filter(e -> e != null)
                .map(e -> e.getType())
                .orElse(null);
    }

    public enum OperationType {
        CREATE("CREATE"), DROP("DROP"), INSERT("INSERT"), DELETE("DELETE"), UPDATE("UPDATE"), SELECT("SELECT"), TEMP("TEMP");

        private String tableType;

        OperationType(String tableType) {
            this.tableType = tableType;
        }

        public String getTableType() {
            return tableType;
        }

        public static OperationType getOperationType(int hiveParserTok) {
            switch (hiveParserTok) {
                case HiveParser.TOK_INSERT:
                case HiveParser.TOK_INSERT_INTO:
                    return INSERT;
                case HiveParser.TOK_DELETE_FROM:
                    return DELETE;
                case HiveParser.TOK_UPDATE_TABLE:
                    return UPDATE;
                case HiveParser.TOK_TABREF:
                    return SELECT;
                case HiveParser.TOK_CREATETABLE:
                    return CREATE;
                case HiveParser.TOK_DROPTABLE:
                    return DROP;
                case HiveParser.TOK_CTE:
                    return TEMP;
                default:
                    return null;
            }

        }
    }


    public static class Table {
        /**
         * 库名
         */
        private String dbName;
        /**
         * 表名
         */
        private String tableName;

        /**
         * ddl类型
         *
         * @see OperationType
         */
        private OperationType OperationType;

        /**
         * 所属sql
         */
        private String sql;

        public Table(String dbName, String tableName, OperationType operationType, String sql) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.OperationType = operationType;
            this.sql = sql;
        }

        public Table() {
        }

        public String getDbName() {
            return dbName;
        }

        public void setDbName(String dbName) {
            this.dbName = dbName;
        }

        public String getTableName() {
            return tableName;
        }

        public HiveSqlParser.OperationType getOperationType() {
            return OperationType;
        }

        public String getSql() {
            return sql;
        }
    }


    public static class InputTable {
        /**
         * 库名
         */
        private String dbName;
        /**
         * 表名
         */
        private String tableName;
        /**
         * 分区列
         */
        private String partitionColumn;
        /**
         * 分区值
         */
        private String partitionValue;

        public InputTable() {
        }

        public InputTable(String dbName, String tableName, String partitionColumn, String partitionValue) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.partitionColumn = partitionColumn;
            this.partitionValue = partitionValue;
        }

        public String getDbName() {
            return dbName;
        }

        public void setDbName(String dbName) {
            this.dbName = dbName;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getPartitionColumn() {
            return partitionColumn;
        }

        public void setPartitionColumn(String partitionColumn) {
            this.partitionColumn = partitionColumn;
        }

        public String getPartitionValue() {
            return partitionValue;
        }

        public void setPartitionValue(String partitionValue) {
            this.partitionValue = partitionValue;
        }
    }

    public static class OutputTable {
        /**
         * 库名
         */
//        private String dbName;
        /**
         * 表名
         */
//        private String tableName;
        /**
         * 分区列
         */
        private String partitionColumn;
        /**
         * 分区值
         */
        private String partitionValue;

        public OutputTable(String partitionColumn, String partitionValue) {
            this.partitionColumn = partitionColumn;
            this.partitionValue = partitionValue;
        }

        public OutputTable() {
        }

        public String getPartitionColumn() {
            return partitionColumn;
        }

        public void setPartitionColumn(String partitionColumn) {
            this.partitionColumn = partitionColumn;
        }

        public String getPartitionValue() {
            return partitionValue;
        }

        public void setPartitionValue(String partitionValue) {
            this.partitionValue = partitionValue;
        }
    }

    public List<OutputTable> getOutputTableList() {
        return outputTableList;
    }

    public List<Table> getTableList() {
        return tableList;
    }

    public List<Table> getTempTableList() {
        return tempTableList;
    }
}
