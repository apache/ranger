1. Test Setup
   1. s3a://mybucket/path/path1/*
      user=path1-r-user, permission=read
      user=path1-rw-user, permission=read,write
   2. s3a://mybucket/path/path2/*
      user=path2-r-user, permission=read
      user=path2-rw-user, permission=read,write
   3. s3a://mybucket/*
      user=all-path-r-user, permission=read
   4. s3a://mybucket/data/tag1/*
      tag=TAG1
      user=tag1-r-user, permission=read
      user=tag1-rw-user, permission=read,write
   5. s3a://mybucket/data/tag2
      tag=TAG2
      user=tag2-r-user, permission=read
      user=tag2-rw-user, permission=read,write
   6. s3a://mybucket/data/ds1
      dataset=ds1
      user=ds1-r-user, permission=read
   7. s3a://mybucket/data/ds2
      dataset=ds2
      user=ds2-r-user, permission=read
