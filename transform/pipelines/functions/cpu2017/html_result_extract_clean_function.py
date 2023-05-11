from pyspark import SparkContext, SparkConf

from pipelines.base.io.handler import IOHandler

from loguru import logger

NULL_STRING = "<null>"


def html_parse(html_contents):
    import bs4
    import re
    for result_id, content_ in html_contents:
        parse_result = {}

        parse_result.update(result_id=result_id)

        template_soup = bs4.BeautifulSoup(content_, features="lxml")
        trim_empties = re.compile(r"\s+")

        title_info_table = template_soup.find(name="table", attrs={'class': 'titlebarcontainer'})
        title_info_table_rows = title_info_table.find_all(name='tr')
        sys_summary, base_result_summary = title_info_table_rows[0].find_all(name="td")
        sys_summary, base_result_summary = sys_summary.find_all(name='p'), base_result_summary.find_all(name='p')

        hardware_vendor, system = (trim_empties.sub(" ", elem.getText()).strip() for elem in sys_summary)
        hardware_vendor = re.sub(r"\(.*\)", "", hardware_vendor)
        hardware_vendor = hardware_vendor.strip()
        parse_result.update(hardware_vendor=hardware_vendor, system=system)

        base_results = []
        for elem in base_result_summary:
            result_value = elem.find(name="span", attrs={"class": "value"})
            result_value_field = result_value.getText()
            hidden = result_value.find(name="span")
            if hidden is None:
                hidden = ""
            else:
                hidden = hidden.getText()
            trim_index = max(0, len(result_value_field) - len(hidden))
            result_value_field = trim_empties.sub("", result_value_field[:trim_index])
            if len(result_value_field) == 0:
                result_value_field = ""
            base_results.append(result_value_field)
        base_results_count = len(base_results)
        base_result = energy_base_result = NULL_STRING
        if base_results_count > 0:
            if base_results_count == 1:
                base_result = base_results[0]
            if base_results_count >= 2:
                base_result = base_results[0]
                energy_base_result = base_results[1]

        peak_result_summary = title_info_table_rows[1].find(name="td")
        peak_result_summary = peak_result_summary.find_all(name='p')
        peak_results = []
        for elem in peak_result_summary:
            result_value = elem.find(name="span", attrs={"class": "value"})
            result_value_field = result_value.getText()
            hidden = result_value.find(name="span")
            if hidden is None:
                hidden = ""
            else:
                hidden = hidden.getText()
            trim_index = max(0, len(result_value_field) - len(hidden))
            result_value_field = trim_empties.sub("", result_value_field[:trim_index])
            if len(result_value_field) == 0:
                result_value_field = ""

            peak_results.append(result_value_field)

        peak_results_count = len(peak_results)
        peak_result = energy_peak_result = NULL_STRING

        if peak_results_count > 0:
            if peak_results_count == 1:
                peak_result = peak_results[0]
            if peak_results_count >= 2:
                peak_result = peak_results[0]
                energy_peak_result = peak_results[1]

        parse_result.update(base_result=base_result, energy_base_result=energy_base_result,
                            peak_result=peak_result, energy_peak_result=energy_peak_result)

        date_info = template_soup.find(name="table", attrs={'class': 'datebar'})
        date_info_rows = date_info.find_all(name='tr')

        license_, test_date = (elem.getText().strip() for elem in date_info_rows[0].find_all(name='td'))
        test_sponsor, hardware_avail_date = (elem.getText().strip() for elem in date_info_rows[1].find_all(name='td'))
        tested_by, software_avail_date = (elem.getText().strip() for elem in date_info_rows[2].find_all(name='td'))

        parse_result.update(license=license_, test_date=test_date, test_sponsor=test_sponsor,
                            hardware_avail_date=hardware_avail_date, tested_by=tested_by,
                            software_avail_date=software_avail_date)

        sys_info = template_soup.find(name='div', attrs={'class': 'infobox'})
        sys_info_tables = sys_info.find_all(name='table')

        info_tables_dict = {}
        for info_table in sys_info_tables:
            table_head = info_table.find(name='thead')
            info_table_head = trim_empties.sub("_", table_head.getText().strip()).lower()

            info_table_body = info_table.find(name='tbody')
            info_pairs = info_table_body.find_all(name="tr")

            info_table_body_dict = {}
            for pair in info_pairs:
                info_k = pair.find(name='th').getText().replace(':', '').strip(".").strip()
                info_k = trim_empties.sub("_", info_k).lower()
                info_v = pair.find(name='td').getText().strip()
                info_table_body_dict[info_k] = info_v

            info_tables_dict[info_table_head] = info_table_body_dict

        parse_result.update(system_info=info_tables_dict)

        results_info = template_soup.find(name='div', attrs={'class': 'resultstable'})
        result_table_titles = results_info.find_all(name='h2')
        result_tables_soup = results_info.find_all(name='table')
        result_tables = list(
            zip((trim_empties.sub(" ", elem.getText()).strip() for elem in result_table_titles), result_tables_soup))

        benchmark_header = "benchmark"
        base_header = "base"
        peak_header = "peak"

        result_tables_dict = {}

        for title, table_soup in result_tables:
            if base_header in title.lower() or peak_header in title.lower():
                headers = table_soup.find(name="thead")
                headers = list(trim_empties.sub("_", elem.getText().strip()).lower() for elem in headers.find_all("th"))

                result_table_rows = []
                body = table_soup.find(name="tbody")
                rows = body.find_all(name="tr")
                for row in rows:
                    result_row_dict = {}
                    for header in headers:
                        result_row_dict[header] = []
                    cols = row.find_all(name="td")
                    for header, col in zip(headers, cols):
                        field = trim_empties.sub("", col.getText())
                        if len(field) == 0:
                            field = NULL_STRING
                        result_row_dict[header].append(field)

                    result_table_rows.append(result_row_dict)
                if base_header in title.lower():
                    result_tables_dict[base_header] = result_table_rows
                elif peak_header in title.lower():
                    result_tables_dict[peak_header] = result_table_rows
            else:
                # parse base results
                def parse_func(result_type=base_header):
                    headers_soup = table_soup.find(name="thead")
                    headers_soup = headers_soup.find_all(name="tr")
                    _, col_headers_soup = headers_soup

                    result_rows = []
                    col_headers = [benchmark_header]
                    col_headers_soup = col_headers_soup.select(f"th.{result_type}col")
                    for elem in col_headers_soup:
                        col_headers.append(trim_empties.sub("_", elem.getText().strip()).lower())

                    body_soup = table_soup.find(name='tbody')
                    rows_soup = body_soup.find_all(name='tr')
                    for row_soup in rows_soup:
                        result_row_dict_ = {}
                        for col_header in col_headers:
                            result_row_dict_[col_header] = []

                        benchmark_col = row_soup.find(name='td', attrs={'class': 'bm'})
                        cols_ = row_soup.select(f"td.{result_type}col")
                        cols_ = (benchmark_col, *cols_)
                        for col_header, c in zip(col_headers, cols_):
                            field_ = trim_empties.sub("", c.getText())
                            if len(field_) == 0:
                                field_ = NULL_STRING
                            result_row_dict_[col_header].append(field_)
                        result_rows.append(result_row_dict_)

                    return result_rows

                result_tables_dict[base_header] = parse_func(base_header)
                result_tables_dict[peak_header] = parse_func(peak_header)

        parse_result.update(result_tables=result_tables_dict)

        notes_soup = template_soup.find_all(lambda tag: tag.name == 'div' and tag.get('class') == ['notes'])

        ordinary_notes_dict = {}

        for note_soup in notes_soup:
            note_title = note_soup.find(name="h2")
            note_title = note_title.getText().strip()
            note_title = trim_empties.sub("_", note_title).lower()
            note_contents = note_soup.find_all(name='pre')
            note_content = ""

            for nc in note_contents:
                note_content += nc.getText()

            ordinary_notes_dict[note_title] = note_content.strip()

        parse_result.update(ordinary_notes=ordinary_notes_dict)

        flag_notes_soup = template_soup.find_all(name='div', attrs={"class": "flags"})

        flag_notes_dict = {}

        for note_soup in flag_notes_soup:
            note_title = note_soup.find(name="h2")
            note_title = note_title.getText().strip()
            note_title = trim_empties.sub("_", note_title).lower()
            note_content = note_soup.getText()

            flag_notes_dict[note_title] = note_content

        parse_result.update(flag_notes=flag_notes_dict)

        yield parse_result


def save_to_mongo_wrapper(sink: IOHandler):
    def save_to_mongo(parse_results):
        sink_results = []
        for parse_result in parse_results:
            sink_results.append(parse_result)

        logger.info(f"{len(sink_results)} docs has sink to mongo.")
        sink.write(sink_results)

    return save_to_mongo


def html_result_extract_clean_app(file_path: IOHandler, sink: IOHandler):
    logger.info("Extracting cpu2017 html results.")
    conf = SparkConf()
    conf.setMaster("local[*]").setAppName("CPU2017_HTML_EXTRACT")

    sc = SparkContext(conf=conf)

    files_rdd = sc.wholeTextFiles(file_path.read(), minPartitions=200)

    id_mapper = files_rdd.map(lambda x: (x[0].split("/")[-1].replace(".html", ""), x[1]))

    html_parser = id_mapper.mapPartitions(lambda it: html_parse(it))

    html_parser.foreachPartition(lambda it: save_to_mongo_wrapper(sink)(it))

    sc.stop()

    logger.info("done.")
