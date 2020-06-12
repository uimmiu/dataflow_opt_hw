# dataflow_opt_hw 使用说明书
##1.打开程序
1.1 将项目复制到本地
1.2 使用idea打开这个项目
1.3 将accessKey、secretKey、bucket、topic设置为对应自己的值，可以根据修改更改剩余的值，如period。
1.4 选中进入Main.scala，点击右键选中Run即可

##2.流程图

flow
st=>start:存在s3中的daas.txt
op1=>operation:kakfa生产者获取s3数据
op2=>operation:kafka消费者处理数据：读取每一行的destination数据。
op3=>operation:kafka消费者将处理过的数据上传至s3的upload2文件夹中。
e=>end
st->op1->op2->op3->e
