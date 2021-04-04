def read_all_properties(config):
    """
    :param config:
    :type config:
    :return:
    :rtype:
    """
    prop_file = open(config, 'r')
    key_values = {}
    for line in prop_file:
        if not line.startswith('#'):
            line_components = line.split('=', 1)
            if line_components[0] and len(line_components) == 2:
                key_values[line_components[0].strip()] = line_components[1].strip()
    prop_file.close()
    return key_values


def write_output(prop, output_obj, file_name):
    type_obj = str(type(output_obj))
    if 'dataframe' in type_obj.lower():
        print('df')
        output_obj.coalesce(1).write.mode('overwrite').format('com.databricks.spark.csv')\
            .option('header', 'true').save(prop['OUTPUT_FOLDER'] + file_name + '.csv')
    elif 'list' in type_obj:
        print('list')
        with open(prop['OUTPUT_FOLDER'] + file_name + '.txt', 'w') as f_out:
            for ind, item in output_obj:
                out_line = str(ind) + ' ' + str(item)
                print(out_line)
                f_out.write(out_line)
                f_out.write('\n')
    else:
        print('other')
        with open(prop['OUTPUT_FOLDER'] + file_name + '.txt', 'w') as f_out:
            print(output_obj)
            f_out.write(str(output_obj))
