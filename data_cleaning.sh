#!/bin/bash

# 检查传入的参数
if [ $# -lt 6 ]; then
    echo "Usage: $0 <DB_HOST> <DB_USER> <DB_PASS> <DB_NAME> <START_DATE> <END_DATE>"
    exit 1
fi

# 获取脚本参数
DB_HOST="$1"        # 数据库主机地址
DB_USER="$2"        # 数据库用户名
DB_PASS="$3"        # 数据库密码
DB_NAME="$4"        # 数据库名
START_DATE="$5"     # 起始日期 (格式: 'yyyy-MM-dd')
END_DATE="$6"       # 结束日期 (格式: 'yyyy-MM-dd')

# 连接数据库并执行数据清洗操作
mysql -h $DB_HOST -u $DB_USER -p$DB_PASS $DB_NAME <<EOF
# 数据清洗：去掉完全重复的数据
INSERT OVERWRITE TABLE user_behavior
SELECT user_id, item_id, category_id, behavior_type, timestamp, datetime
FROM user_behavior
GROUP BY user_id, item_id, category_id, behavior_type, timestamp, datetime;

# 数据清洗：时间戳格式化成 datetime
INSERT OVERWRITE TABLE user_behavior
SELECT user_id, item_id, category_id, behavior_type, timestamp, 
       FROM_UNIXTIME(timestamp, '%Y-%m-%d %H:%i:%s')
FROM user_behavior;

# 查看时间是否有异常值
SELECT DATE(datetime) AS day
FROM user_behavior
GROUP BY DATE(datetime)
ORDER BY day;

# 数据清洗：去掉时间异常的数据
INSERT OVERWRITE TABLE user_behavior
SELECT user_id, item_id, category_id, behavior_type, timestamp, datetime
FROM user_behavior
WHERE CAST(datetime AS DATE) BETWEEN '$START_DATE' AND '$END_DATE';

# 查看 behavior_type 是否有异常值
SELECT behavior_type
FROM user_behavior
GROUP BY behavior_type;
EOF

echo "数据清洗操作完成！"
