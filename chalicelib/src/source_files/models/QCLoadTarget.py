class QCLoadTarget:
    def __init__(
        self,
        emissionsqc_load_target_id,
        source_name_id,
        reporting_year,
        layer_id,
        target_tab,
        row_title_cell,
        anticipated_row_title,
        data_ref_1990,
        emission_parameters,
        report_row_id
    ):
        self._emissionsqc_load_target_id = emissionsqc_load_target_id
        self._source_name_id = source_name_id
        self._reporting_year = reporting_year
        self._layer_id = layer_id
        self._target_tab = target_tab
        self._row_title_cell = row_title_cell
        self._anticipated_row_title = anticipated_row_title
        self._data_ref_1990 = data_ref_1990
        self._emission_parameters = emission_parameters
        self._report_row_id = report_row_id

    def get_emissionsqc_load_target_id(self):
        return self._emissionsqc_load_target_id

    def get_source_name_id(self):
        return self._source_name_id

    def get_reporting_year(self):
        return self._reporting_year

    def get_layer_id(self):
        return self._layer_id

    def get_target_tab(self):
        return self._target_tab

    def get_row_title_cell(self):
        return self._row_title_cell

    def get_anticipated_row_title(self):
        return self._anticipated_row_title

    def get_data_ref_1990(self):
        return self._data_ref_1990

    def get_emission_parameters(self):
        return self._emission_parameters

    def get_report_row_id(self):
        return self._report_row_id