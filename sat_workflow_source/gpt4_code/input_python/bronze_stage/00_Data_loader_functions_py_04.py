
#explode column logic
def specified_column(df:"DataFrame",explode_column:"String"):
    
        dyn_class=explode_column+'_dyn_class'
        href=explode_column+'_href'
        stat_type = explode_column+ "_stat_type"
        
        df=(df.withColumn(explode_column,explode_outer(col(explode_column)))
            .withColumn(href,col(explode_column)['_href'])
            .withColumn(href,split(col(href),'#').getItem(1)))
        
        if not explode_column.endswith("Function_occ"):
            df=(df.withColumn(dyn_class,col(explode_column)['_dyn_class']).drop(explode_column))
        
        else:
            df=(df.withColumn(stat_type,col(explode_column)['_stat_type']).drop(explode_column))
        
        return df

