# 测试结果

## 功能测试
* put 设置

![put](images/test-func-put.png)

* get 获取

![get](images/test-func-get.png)

* del 删除

![del](images/test-func-del.png)

* list 列出所有记录

![list](images/test-func-list.png)

* ins 批量插入

![ins](images/test-func-ins.png)

* ver 校验（key==value）

![ver](images/test-func-verify.png)

* clr 清除记录

![clr](images/test-func-clear.png)

## 性能测试
* platform win10-mingw
* cpu      i5-8550U
* disk     ssd

### 批量插入1000,0000条key值递增的数据(单位：usec-微妙)
* 1000w条数据4.37秒插入完成，每秒228w左右
* 1000w条数据的数据文件约为331M

![ins](images/test-perf-ins.png)
![ins-filesize](images/test-perf-ins-filesize.png)
