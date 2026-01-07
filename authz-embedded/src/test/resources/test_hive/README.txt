1. Test Setup
   1. database=mydb, table=tbl1, column=*
      user=tbl1-r-user, permission=select
      user=tbl1-rw-user, permission=select,update
   2. database=mydb, table=tbl2, column=*
      user=tbl2-r-user, permission=select
      user=tbl2-rw-user, permission=select,update
   3. database=mydb, table=*, column=*
      user=all-tbl-r-user, permission=select
   4. database=mydb, table=tbl_tag1, column=*
      tag=TAG1
      user=tag1-r-user, permission=select
      user=tag1-rw-user, permission=select,update
   5. database=mydb, table=tbl_tag2, column=*
      tag=TAG2
      user=tag2-r-user, permission=select
      user=tag2-rw-user, permission=select,update
   6. database=mydb, table=tbl_ds1, column=*
      dataset=ds1
      user=ds1-r-user, permission=select
   6. database=mydb, table=tbl_ds2, column=*
      dataset=ds2
      user=ds2-r-user, permission=select
   7. database=mydb, table=tbl_ds3, column=col1,col2,col3
      dataset=ds3, col1=mask_hash, row-filter=(year=current_year())
      user=ds3-r-user, permission=select
