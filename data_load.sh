#!/bin/bash
# 检查传入的参数
if [ $# -lt 4 ]; then
    echo "Usage: $0 <DB_HOST> <DB_USER> <DB_PASS> <DB_NAME>"
    exit 1
fi

# 获取脚本参数
DB_HOST="$1"        # 数据库主机地址
DB_USER="$2"        # 数据库用户名
DB_PASS="$3"        # 数据库密码
DB_NAME="$4"        # 数据库名

# 连接数据库并执行建表操作
mysql -h $DB_HOST -u $DB_USER -p$DB_PASS $DB_NAME <<EOF
DROP TABLE IF EXISTS user_behavior;

CREATE TABLE user_behavior (
    user_id STRING COMMENT '用户ID',
    item_id STRING COMMENT '商品ID',
    category_id STRING COMMENT '商品类目ID',
    behavior_type STRING COMMENT '行为类型, 枚举类型, 包括(pv, buy, cart, fav)',
    timestamp INT COMMENT '行为时间戳',
    datetime STRING COMMENT '行为时间'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';

EOF

# 加载数据到表中
mysql -h $DB_HOST -u $DB_USER -p$DB_PASS $DB_NAME <<EOF
LOAD DATA LOCAL INFILE '/home/getway/UserBehavior.csv'
OVERWRITE INTO TABLE user_behavior;
EOF

echo "操作完成！"
