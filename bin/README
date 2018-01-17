# 特征离散化中间线下测试工具

## what is it?

这是用来做线下特征离散化配置文件解析以及单行样本处理结果的测试工具
也就是说这个只能表示离散化以及个性化特征生成，不包括组合特征以及one-hot编码


## how to use ?

./start.sh  ./feature_process_select.cfg   ./sample_in  ./sample_out

输出结果 sample_out 是离散化后的样本
         result 是取第一行样本 进行离散化配置、实际取值、离散化后结果的比对


## 配置文件

1. 连续特征离散化：
    - 等区间离散化 如：`DSpic_num=cont_mp1:0,1,16:-1:20` 
    - 自定义区间离散化 如 `DSprice=cont_mp2:-9999~0,0~220.0,220.0~1000:-1:1000`

2. 离散特征不做离散化
    如：`DSroom=cate_map:-1:16` 

3. 个性化特征：
    - 用户对某特征最偏好的特征值 如 `PLfdroom~1~n=pertop:-1:20`
    - 用户对某特征最偏好特征值的偏好度 `PLfdroom~1~v=pertop:-1:1000`
    - 帖子某特征的特征值对应用户在该特征值上的偏好度 `PLfdroom=permatch:-1:1000`
    - 帖子的某特征值是否用户最偏好 `PLfdroom~1~c=permatch:-1:3`
    - 帖子某特征值对应用户偏好度与用户最偏好特征值对应偏好度的差距 `PLfdroom~1~g=permatch:-1:1000`


4. 单特征选择

single=DSprice,DSarea,DSroom,DShall,DStoilet,DSorientation,DSrenovation,DShouse_type,DSinfosource,DSunit_price,DSprice_range,DSarea_range,DScommunity,DSdistrict,DSbusi_circle,DSadddays,DSpic_num,DSfacility_num,HChctr,RCrctr,PLfdroom,PLfdhall,PLfdtoilet,PLfdorientation,PLfdrenovation,PLfdcommunity,PLfdbusicircle,PLfdpricerange,PLfdarearange,PLfdroom~1~c,PLfdhall~1~c,PLfdtoilet~1~c,PLfdorientation~1~c,PLfdrenovation~1~c,PLfdcommunity~1~c,PLfdbusicircle~1~c,PLfdpricerange~1~c,PLfdarearange~1~c,PLfdroom~1~g,PLfdhall~1~g,PLfdtoilet~1~g,PLfdorientation~1~g,PLfdrenovation~1~g,PLfdcommunity~1~g,PLfdbusicircle~1~g,PLfdpricerange~1~g,PLfdarearange~1~g,RPrdroom,RPrdhall,RPrdtoilet,RPrdorientation,RPrdrenovation,RPrdcommunity,RPrdbusicircle,RPrdpricerange,RPrdarearange,RPrdroom~1~c,RPrdhall~1~c,RPrdtoilet~1~c,RPrdorientation~1~c,RPrdrenovation~1~c,RPrdcommunity~1~c,RPrdbusicircle~1~c,RPrdpricerange~1~c,RPrdarearange~1~c,RPrdroom~1~g,RPrdhall~1~g,RPrdtoilet~1~g,RPrdorientation~1~g,RPrdrenovation~1~g,RPrdcommunity~1~g,RPrdbusicircle~1~g,RPrdpricerange~1~g,RPrdarearange~1~g

5. 组合特征选择

merge=DSlongitude*DSlatitude,PLfdroom~1~n*DSroom,PLfdhall~1~n*DShall,PLfdtoilet~1~n*DStoilet,PLfdorientation~1~n*DSorientation,PLfdrenovation~1~n*DSrenovation,PLfdcommunity~1~n*DScommunity,PLfdbusicircle~1~n*DSbusi_circle,PLfdpricerange~1~n*DSprice_range,PLfdarearange~1~n*DSarea_range,PLfdroom~1~v*PLfdroom,PLfdhall~1~v*PLfdhall,PLfdtoilet~1~v*PLfdtoilet,PLfdorientation~1~v*PLfdorientation,PLfdrenovation~1~v*PLfdrenovation,PLfdcommunity~1~v*PLfdcommunity,PLfdbusicircle~1~v*PLfdbusicircle,PLfdpricerange~1~v*PLfdpricerange,PLfdarearange~1~v*PLfdarearange,RPrdroom~1~n*DSroom,RPrdhall~1~n*DShall,RPrdtoilet~1~n*DStoilet,RPrdorientation~1~n*DSorientation,RPrdrenovation~1~n*DSrenovation,RPrdcommunity~1~n*DScommunity,RPrdbusicircle~1~n*DSbusi_circle,RPrdpricerange~1~n*DSprice_range,RPrdarearange~1~n*DSarea_range,RPrdroom~1~v*RPrdroom,RPrdhall~1~v*RPrdhall,RPrdtoilet~1~v*RPrdtoilet,RPrdorientation~1~v*RPrdorientation,RPrdrenovation~1~v*RPrdrenovation,RPrdcommunity~1~v*RPrdcommunity,RPrdbusicircle~1~v*RPrdbusicircle,RPrdpricerange~1~v*RPrdpricerange,RPrdarearange~1~v*RPrdarearange

6. 个性化取偏好度配置

match_dict={PLfdroom:DSroom,PLfdhall:DShall,PLfdtoilet:DStoilet,PLfdorientation:DSorientation,PLfdrenovation:DSrenovation,PLfdcommunity:DScommunity,PLfdbusicircle:DSbusi_circle,PLfdpricerange:DSprice_range,PLfdarearange:DSarea_range,RPrdroom:DSroom,RPrdhall:DShall,RPrdtoilet:DStoilet,RPrdorientation:DSorientation,RPrdrenovation:DSrenovation,RPrdcommunity:DScommunity,RPrdbusicircle:DSbusi_circle,RPrdpricerange:DSprice_range,RPrdarearange:DSarea_range}




离散化配置说明:

1. cont_mp1 后面冒号分隔的4段，第一段是离散化类型，第二段是离散化方式，等区间的（起始值，步长，最大区间），第三段默认值，第四段线上预分配内存空间（预期取值种类）

2. cont_mp2 后面冒号分隔的4段，第一段是离散化类型，第二段是离散化方式，自定义区间（左闭右开），第三段默认值，第四段线上预分配内存空间（预期取值种类）

3. cate_map 后面冒号分隔的3段，第一段是离散化类型（其实没有离散化），第二段是默认值，第三段是线上预分配内存空间（预期取值种类）

4. feature~1~n feature~1~v  对应pertop， 后面冒号分隔的3段含义与cate_map相同, 等号左边的特征名格式，两个波浪线之间的数字表示降序第几偏好，n表示偏好特征值，v表示偏好度

5. feature, feature~1~c feature~1~g  对应permatch， 后面冒号分隔的3段含义与cate_map相同, 等号左边的特征名格式，两个波浪线之间的数字表示降序第几偏好，c表示combine（是否最偏好），g表示gap（与最偏好度之间的差距）, 如果没有波浪线，那么表示取匹配偏好度

6. single 配置了使用哪些单特征，对于当前的 GBDT 和 XGBoost 模型，只有 single 生效；
7. merge 配置了如何使用组合特征，只有当前的LR模型使用
8. match_dict 配置格式： 大括号包裹的kv对集合，其实是一种json格式，k-v对的k是个性化特征名，v是对应的被偏好特征. ** 如果要使用个性化特征中的permatch（包括无波浪、~c, ~g）都必须配置match_dict

