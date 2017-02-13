## Recommender System

---
#### What is Recommender System
+ Systems that attempt to predict item that ysers may be interested in
+ Systems that help people find information that may interest them


---
#### Algorithms used for Recommender System
+ User Collaborative Filtering (User CF)
  - A form of **collaborative filtering (协同过滤算法)** based on the similarity between users calculated using people's rating of those items
+ Item Collaborative Filtering (Item CF)
  - A form of **collaborative filtering** based on the similarity between items calculated using people's rating of those items
+ etc.


---
Item CF
+ Build co-occurrence matrix
+ Build rating matrix
+ Matrix computation to get recommending result


---
* 此项目我将运用Netflix数据, 给用户推荐他们之前喜欢的电影的相似电影。
* 运用基于物品的协同过滤算法，从 Netflix 的数据中构得到用户对电影的评分矩阵，再得到电影的同现矩阵（也就是电影之间的相似度矩阵）， 最后合并同现矩阵和评分矩阵，得到推荐列表。
* 此项目我们将实现4个Map Reduce Job连接所有的流程, 实现最重要的Map Reduce 版本矩阵相乘。
