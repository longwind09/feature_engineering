# feature_engineering
feature engineering framework of machine learning process

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


## 支持单机和分布式
