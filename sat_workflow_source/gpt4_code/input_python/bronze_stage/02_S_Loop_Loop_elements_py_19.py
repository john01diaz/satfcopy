
(
    layer_with_loop_elements
    .write
    .format("delta")
    .save(
        path="dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_bronze/CS_Layer_Loop_Loop_elements"
       ,mode = "overwrite"
    )
)