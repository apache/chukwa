GLOG_logtostderr=1 /CaffeOnSpark/caffe-public/.build_release/tools/convert_imageset \
   --resize_height=200 --resize_width=1000 --shuffle --encoded \
   /caffe-test/train/data/ \
   /caffe-test/train/data/labels.txt \
   /caffe-test/train/lmdb

