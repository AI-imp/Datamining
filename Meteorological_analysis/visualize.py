import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

# 设置中文字体
font_path = './weiruanyaheicuti/粗体微软雅黑/cutiweiruanyahei.ttf'
font_prop = fm.FontProperties(fname=font_path)
# 读取CSV表格
df = pd.read_csv('./so2_20.csv')

# 将"监测时间"列转换为时间格式
df['监测时间'] = pd.to_datetime(df['监测时间'])

# 将“监测时间”列设置为DataFrame的索引
df.set_index('监测时间', inplace=True)

# 绘制柱状图
ax = df.plot(kind='bar', y='SO2监测浓度(μg/m3)', figsize=(10, 6), color='blue')

# 设置图表标题和x轴、y轴标签
ax.set_title('SO2监测浓度', fontsize=16,fontproperties=font_prop)
ax.set_xlabel('监测时间', fontsize=14,fontproperties=font_prop)
ax.set_ylabel('SO2监测浓度', fontsize=14,fontproperties=font_prop)

# 显示图表
plt.show()
