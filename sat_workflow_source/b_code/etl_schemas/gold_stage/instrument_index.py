from enum import Enum


class Instrument_Index(
        Enum):
    ACCURACY_SPEED_OUTPUT = 'accuracy speed output ±'
    ACTUATING_TIME = 'actuating time'
    ACTUATOR_INSTALLATION_POSITION = 'actuator installation position'
    ANALYZER_90_PERCENTAGE_TIME_OR_LENGTH_OF_ANALYSIS = 'analyzer 90 percentage time or length of analysis'
    ANALYZER_CALIBRATION_CURVE = 'analyzer calibration curve'
    ANALYZER_ERROR_TOLERANCE = 'analyzer error tolerance'
    ANALYZER_RANGE_OF_TEMPERATURE_COMPENSATION = 'analyzer range of temperature compensation'
    APERTURE_ANGLE_VENTURI_TUBE = 'aperture angle venturi tube'
    APERTURE_DIAMETER_D = 'aperture diameter d'
    AREA = 'area'
    AREAPATH = 'areapath'
    ASSEMBLY_CATEGORY = 'assembly category'
    ASSEMBLY_IDENTIFIER = 'assembly identifier'
    ASSEMBLY_LOCATION = 'assembly location'
    AUXILIARY_MATERIALS = 'auxiliary materials'
    AUXILIARY_POWER_SIZE = 'auxiliary power size'
    AUXILIARY_POWER_TYPE = 'auxiliary power type'
    AVERAGE_ANNUAL_TEMPERATURE = 'average annual temperature'
    BLUFF_BODY_FORM = 'bluff body form'
    BLUFF_BODY_MATERIAL = 'bluff body material'
    BREAKDOWN_TORQUE_RATIO_MK_PER_MN = 'breakdown torque ratio mk per mn'
    BYPASS_AMOUNT_MAXIMUM = 'bypass amount maximum'
    BYPASS_AMOUNT_MINIMUM = 'bypass amount minimum'
    BYPASS_FOR_MOTOR_NETWORK_OPERATION = 'bypass for motor network operation'
    CABLE_LENGTH_SYSTEM = 'cable length system'
    CALCULATED_KV_VALUE = 'calculated kv-value'
    CALCULATED_MAXIMUM_SOUND_PRESSURE_LEVEL = 'calculated maximum sound pressure level'
    CALIBRATION_FROM = 'calibration from'
    CALIBRATION_TO = 'calibration to'
    CAPILLARY_LENGTH = 'capillary length'
    CAPILLARY_LIQUID = 'capillary liquid'
    CERTIFICATE = 'certificate'
    CHARACTERISTIC = 'characteristic'
    CHARACTERISTIC_LINE = 'characteristic line'
    CHARACTERISTIC_NOISE_VALUE_ZY = 'characteristic noise value zy'
    CHARACTERISTIC_POSITIONER = 'characteristic positioner'
    CIRCUIT_ARRANGEMENT = 'circuit arrangement'
    CIRCUIT_ARRANGEMENT2 = 'circuit arrangement2'
    CIRCUIT_TYPE = 'circuit type'
    CLASSIFICATION = 'classification'
    CLASSIFICATION_BY = 'classification by'
    CLASSNAME = 'classname'
    COMMENT = 'comment'
    CONE_MATERIAL = 'cone material'
    CONE_TYPE = 'cone type'
    CONSTRUCTION_YEAR = 'construction year'
    CONTACT_DESIGN = 'contact design'
    CONTROL_CABLE_HEATING = 'control cable heating'
    CONTROL_CABLE_LENGTH_HEATING = 'control cable length heating'
    CONTROL_CABLE_MAXIMUM_NUMBER_OF_CABLE_INLETS = 'control cable maximum number of cable inlets'
    CONTROL_CABLE_MAXIMUM_WIRE_CROSS_SECTION = 'control cable maximum wire cross section'
    CONTROL_CABLE_SREWED_JOINT = 'control cable srewed joint'
    CONTROL_DEVICE_DESIGN = 'control device design'
    CONTROL_INPUT_RANGE = 'control input range'
    CONTROL_TRIPPING_VOLTAGE = 'control tripping voltage'
    CONTROL_VALVE_CERTIFICATION = 'control valve certification'
    CONTROLLER_MAXIMUM = 'controller maximum'
    CONTROLLER_MINIMUM = 'controller minimum'
    CONTROLLERS_MAXIMUM_NUMBER_OF_CABLE_INLETS = 'controllers maximum number of cable inlets'
    CONTROLLERS_MAXIMUM_WIRE_CROSS_SECTION = 'controllers maximum wire cross section'
    COOLING_FINS = 'cooling fins'
    COUNTER = 'counter'
    COUNTER_DESIGN = 'counter design'
    COUNTER_TYPE = 'counter type'
    CUT_OUT_TORQUE = 'cut out torque'
    DCSIO = 'dcsio'
    DEFAULT_COMPONENT_NAME = 'default component name'
    DEFAULT_SYMBOL = 'default symbol'
    DEFAULT_SYMBOL_LIBRARY = 'default symbol library'
    DENSITY_AT_15C = 'density at 15c'
    DESCRIPTION = 'description'
    DESIGN = 'design'
    DESIGN_GAUGE_PRESSURE = 'design gauge pressure'
    DESIGNATION = 'designation'
    DETECTION_TYPE = 'detection type'
    DETONATION_HAZARD_PROTECTION_TYPE_HEATER = 'detonation hazard protection type heater'
    DETONATION_HAZARD_PROTECTION_TYPE_MOTOR = 'detonation hazard protection type motor'
    DEVICE_TYPE_MACHINE_MONITORING = 'device type machine monitoring'
    DEVICE_TYPE = 'device_type'
    DIFFERENTIAL_PRESSURE_RANGE = 'differential pressure range'
    DIGITAL_FUNCTION = 'digital function'
    DISPLAY_TYPE = 'display type'
    DN = 'dn'
    DP_ARMATURE_CHARACTERISTIC_XT = 'dp armature characteristic xt'
    DRIVE_SYSTEM_FOR = 'drive system for'
    DRIVE_WITH_HEATING = 'drive with heating'
    EDGE_CHANGE = 'edge change'
    EFFECT_OF_DP = 'effect of dp'
    EFFECTIVE_DIRECTION = 'effective direction'
    EFFICIENCY_AT_NOMINAL_LOAD = 'efficiency at nominal load'
    EFFICIENCY_AT_NOMINAL_POWER_AND_MINIMUM_ROTATIONAL_SPEED = 'efficiency at nominal power and minimum rotational ' \
                                                               'speed'
    EFFICIENCY_AT_NOMINAL_POWER_AND_NOMINAL_ROTATIONAL_SPEED = 'efficiency at nominal power and nominal rotational ' \
                                                               'speed'
    ELECTRICAL_CONNECTION_DESIGN = 'electrical connection design'
    ELEMENT_FRACTURE = 'element fracture'
    ELEMENT_TYPE = 'element type'
    ENCLOSURE_TYPE_CLASS = 'enclosure type class'
    ENGINE_IDENTIFICATION_NUMBER = 'engine identification number'
    EX_I = 'ex (i)'
    EX_ID = 'ex-id'
    EXTENSION = 'extension'
    EXTERNAL_SETPOINT_SPECIFICATION = 'external setpoint specification'
    FACILITY_BLOCK_DEFAULT = 'facility block default'
    FAIL_SAFE = 'fail safe'
    FD_OUTPUT_SIGNAL = 'fd output signal'
    FILLING_MEDIUM_FOR_ATTENUATION = 'filling medium for attenuation'
    FILTER_FOR_HARMONIC_CURRENTS = 'filter for harmonic currents'
    FINAL_ACCEPTANCES = 'final acceptances'
    FINE_ADJUSTMENT_VALVE = 'fine adjustment valve'
    FLASH_SIGNALING = 'flash signaling'
    FLOAT_TYPE_FORM = 'float type form'
    FLOAT_TYPE_MATERIAL = 'float-type material'
    FLOW_DIRECTION = 'flow direction'
    FLOW_MAXIMUM = 'flow maximum'
    FLOW_MINIMUM = 'flow minimum'
    FOR_PRE_OR_POST_PRESSURE_OSCILLATION = 'for pre or post pressure oscillation'
    FORMATNAME = 'formatname'
    FRAME_SIZE = 'frame size'
    FUNCTION = 'function'
    FUNCTION_AT_MAXIMUM = 'function at maximum'
    FUNCTION_AT_MINIMUM = 'function at minimum'
    GALVANIC_SEPARATION = 'galvanic separation'
    GLAND_NUT_LUBRICATION = 'gland nut lubrication'
    GLAND_NUT_MATERIAL = 'gland nut material'
    GLAND_NUT_TYPE = 'gland nut type'
    HANDWHEEL_POSITION = 'handwheel position'
    HEAD_END_MATERIAL = 'head end material'
    HEAT_GENERATOR_INSTALLATION_POSITION = 'heat generator installation position'
    HEATING = 'heating'
    HEATING_BUNDLE_DIAMETER = 'heating bundle diameter'
    HEATING_LIMITER = 'heating limiter'
    HEATING_REMARK = 'heating remark'
    HEATING_SETTINGS_ANTIFREEZE = 'heating settings antifreeze'
    HOOK_UP_FIGURE = 'hook-up figure'
    HOUSING_MATERIAL = 'housing material'
    HOUSING_PRESSURE_STAGE = 'housing pressure stage'
    HOUSING_TYPE = 'housing type'
    IMPULSE_FACTOR = 'impulse factor'
    INPUT_SIGNAL = 'input signal'
    INPUT_SIGNAL_EXPLOSION_PROTECTION = 'input signal explosion protection'
    INSERT_LENGTH = 'insert length'
    INSTALLATION_LAYER = 'installation layer'
    INSTALLATION_LENGTH = 'installation length'
    INSTALLATION_SITE = 'installation site'
    INSTRUMENT_LAYOUT = 'instrument layout'
    INSTRUMENT_LAYOUT_PAGE_NUMBER = 'instrument layout page number'
    INSULATION_CLASS = 'insulation class'
    INTERRUPTING_CURRENT_LIMIT_XIN = 'interrupting current limit xin'
    INTRINSIC_SAFETY_CAPACITY = 'intrinsic safety capacity'
    INTRINSIC_SAFETY_CURRENT = 'intrinsic safety current'
    INTRINSIC_SAFETY_DEVICE_TYPE = 'intrinsic safety device type'
    INTRINSIC_SAFETY_INDUCTIVITY = 'intrinsic safety inductivity'
    INTRINSIC_SAFETY_POWER = 'intrinsic safety power'
    INTRINSIC_SAFETY_VOLTAGE = 'intrinsic safety voltage'
    ISALOCATION = 'isalocation'
    ITEM_DESIGNATION_BLOCK_DEFAULT = 'item designation block default'
    JUNCTION_BOX = 'junction_box'
    KV_VALUE_AT_KVS_NOMINAL_STROKE = 'kv value at kvs nominal stroke'
    KVS_VALUE = 'kvs-value'
    LEAKAGE_RATE_PERCENT_OF_KVS = 'leakage rate (% of kvs)'
    LENGTH_CONNECTION_CABLE = 'length connection cable'
    LENGTH_OF_DISPLACER = 'length of displacer'
    LENGTH_OF_SUSPENSION_DEVICE = 'length of suspension device'
    LENGTH_OF_TRACE_HEATING = 'length of trace heating'
    LIMIT_SWITCH = 'limit switch'
    LIMIT_SWITCH_CLOSE = 'limit switch close'
    LIMIT_SWITCH_OPEN = 'limit switch open'
    LIMIT_SWITCH_SIGNALED = 'limit switch signaled'
    LIMITER_RELEASE = 'limiter release'
    LOCATION_BLOCK_DEF = 'location block def.'
    LOOP_DIAGRAM_LEVEL = 'loop diagram level'
    LOOPDWGCODE = 'loopdwgcode'
    LOOPFUNC = 'loopfunc'
    LOOPNO = 'loopno'
    LSERVICE1 = 'lservice1'
    MACHINE_TYPE = 'machine type'
    MANUAL_ACTUATION = 'manual actuation'
    MANUAL_OPERATION = 'manual operation'
    MANUFACTURER = 'manufacturer'
    MASS_LIMITATION_BEFORE_AGR = 'mass limitation before agr'
    MAXIMUM_CABLE_LENGTH_OF_CONVERTER_MOTOR = 'maximum cable length of converter motor'
    MAXIMUM_COOLANT_REQUIREMENTS_AT_NOMINAL_LOAD = 'maximum coolant requirements at nominal load'
    MAXIMUM_MOTOR_POWER_IN_CONVERTER_OPERATION = 'maximum motor power in converter operation'
    MAXIMUM_NUMBER_OF_CABLE_INLETS = 'maximum number of cable inlets'
    MAXIMUM_OF_HARMONIC_VOLTAGE = 'maximum of harmonic voltage'
    MAXIMUM_PERMISSIBLE_SOUND_PRESSURE_LEVEL = 'maximum permissible sound pressure level'
    MAXIMUM_POWER_LOSS = 'maximum power loss'
    MAXIMUM_RATED_MOTOR_CURRENT = 'maximum rated motor current'
    MAXIMUM_RATED_MOTOR_VOLTAGE = 'maximum rated motor voltage'
    MAXIMUM_WIRE_CROSS_SECTION = 'maximum wire cross section'
    MEASURED_QUANTITY = 'measured quantity'
    MEASURED_VALUE_DISPLAY = 'measured value display'
    MEASURING_PROCEDURE = 'measuring procedure'
    MEASURING_RANGE = 'measuring range'
    MEASURING_RANGE_FROM = 'measuring range from'
    MEASURING_RANGE_TO = 'measuring range to'
    MEDIUM_CONNECTION_DESIGN = 'medium connection design'
    METER_TYPE = 'meter type'
    METER_METERING_ELEMENT_MATERIAL = 'meter/metering element material'
    METERING_CONE_LENGTH = 'metering cone length'
    METERING_CONE_MATERIAL = 'metering cone material'
    METHOD_OF_CONNECTION = 'method of connection'
    METHOD_OF_MEASUREMENT_MACHINE_MONITORING = 'method of measurement machine monitoring'
    MINIMUM_AMOUNT_FOR_SALE = 'minimum amount for sale'
    MINIMUM_DESIGN_TEMPERATURE = 'minimum design temperature'
    MODEL = 'model'
    MODULE_NAME = 'module name'
    MOTOR_PROTECTION_IN_CONVERTER_OPERATION = 'motor protection in converter operation'
    MOTOR_PROTECTION_IN_NETWORK_OPERATION = 'motor protection in network operation'
    MT_HEAD_ASSEMBLY = 'mt head assembly'
    NETWORK_PROTECTION = 'network protection'
    NOISE_PROTECTION_MEASURES = 'noise protection measures'
    NOMINAL_POWER = 'nominal power'
    NOMINAL_TORQUE = 'nominal torque'
    NTH_TIME = 'nth time'
    NUMBER = 'number'
    NUMBER_OF_SWITCHING_CONTACTS = 'number of switching contacts'
    OCCUPATION_DENSITY = 'occupation density'
    ON_STATE_LIMIT_VALUE_1 = 'on state limit value 1'
    ON_STATE_LIMIT_VALUE_2 = 'on state limit value 2'
    OPENING_CLOSING_TIMES = 'opening/closing times'
    OPERATING_PRESSURE_AT_FEEDBACK_LOCATION_MAXIMUM = 'operating pressure at feedback location maximum'
    OPERATING_PRESSURE_AT_FEEDBACK_LOCATION_NORMAL = 'operating pressure at feedback location normal'
    OPERATINGPRINC = 'operatingprinc'
    ORIFICE_FILLING_MEDIUM = 'orifice filling medium'
    OUTPUT_FREQUENCY_CONTROL_RANGE_FROM = 'output frequency control range from'
    OUTPUT_FREQUENCY_CONTROL_RANGE_TO = 'output frequency control range to'
    OUTPUT_SIGNAL = 'output signal'
    OUTPUT_SIGNAL_EXPLOSION_PROTECTION = 'output signal explosion protection'
    OUTPUT_SPEED = 'output speed'
    OVERALL_HEIGHT = 'overall height'
    OVERALL_LENGTH = 'overall length'
    PA_BUS_CURRENT_INPUT = 'pa bus current input'
    PA_BUS_MAX_SHORT_CIRCUIT_CURRENT = 'pa bus max. short-circuit current'
    PA_BUS_TERMINAL = 'pa bus terminal'
    PARTS_CODE = 'parts code'
    PERFORMANCE_FACTOR_AT_FULL_LOAD = 'performance factor at full load'
    PIDNO = 'pidno'
    POSITION = 'position'
    POSITION_FEEDBACK_ANALOG = 'position feedback analog'
    POSITION_WITHOUT_POWER = 'position without power'
    POWER_CABLE_MAXIMUM_NUMBER_OF_CABLE_INLET = 'power cable maximum number of cable inlet'
    POWER_CABLE_MAXIMUM_WIRE_CROSS_SECTION = 'power cable maximum wire cross section'
    POWER_CABLE_SCREWED_JOINT = 'power cable screwed joint'
    POWER_CONSUMPTION = 'power consumption'
    POWER_SUPPLY_FROM_MODULE = 'power supply from module'
    PRESSURE_GAUGE_SHUT_OFF = 'pressure gauge shut-off'
    PRESSURE_LEVEL = 'pressure level'
    PRESSURE_RECOVER_FACTOR_FL = 'pressure recover factor fl'
    PRIMARY_PRESSURE_REDUCER = 'primary pressure reducer'
    PROBE_LENGTH = 'probe length'
    PROBE_LENGTH_LEVEL = 'probe length level'
    PROCESSEQUIPMENT = 'processequipment'
    PROCESSLINES = 'processlines'
    PTB_NUMBER = 'ptb number'
    PTC_THERMISTOR_TEMPERATURE_SENSOR = 'ptc thermistor temperature sensor'
    PULSE_NUMBER_INPUT_LINE_SIDE_GR = 'pulse number input line side gr'
    PULSE_NUMBER_OUTPUT_TO_MOTOR_WR = 'pulse number output to motor wr'
    PUMP_FOR_SAMPLE = 'pump for sample'
    PUMP_FOR_SAMPLE_PREPARATION = 'pump for sample preparation'
    QUARTER_CIRCLE_NOZZLE_RADIUS_R = 'quarter circle nozzle radius r'
    RADIO_INTERFERENCE_LEVEL = 'radio interference level'
    RATED_APPARENT_POWER = 'rated apparent power'
    RATED_ASYNCHRONOUS_SPEED = 'rated asynchronous speed'
    RATED_CURRENT = 'rated current'
    RATED_CURRENT_OF_DRIVE = 'rated current of drive'
    RATED_POWER = 'rated power'
    RATED_POWER_AT_NOMINAL_OPERATION = 'rated power at nominal operation'
    RATED_POWER_OF_DRIVE = 'rated power of drive'
    RATED_POWER_PER_METER = 'rated power per meter'
    RATED_TORQUE_MN = 'rated torque mn'
    RED_MARKING_DESIGN = 'red marking design'
    RED_MARKING_SETTING = 'red marking setting'
    REFERENCE_TEMPERATURE = 'reference temperature'
    REFERENCE_VESSEL_MATERIAL = 'reference vessel material'
    REMARKS = 'remarks'
    REQUIRED_INLET_LENGTH = 'required inlet length'
    RESISTANCE_THERMOCOUPLE_TYPE = 'resistance thermocouple type'
    RESTRICTION_ORIFICE_DIAMETER = 'restriction orifice diameter'
    RESTRICTION_ORIFICE_MATERIAL = 'restriction orifice material'
    RESTRICTOR_DESIGN = 'restrictor design'
    RING_CHAMBER_DIMENSION_A = 'ring chamber dimension a'
    RING_CHAMBER_DIMENSION_D = 'ring chamber dimension d'
    RING_CHAMBER_DIMENSION_E = 'ring chamber dimension e'
    RING_CHAMBER_MATERIAL = 'ring chamber material'
    ROTATIONAL_SPEED_OF_DRIVE = 'rotational speed of drive'
    ROTATIONAL_SPEED_RANGE_FROM = 'rotational speed range from'
    ROTATIONAL_SPEED_RANGE_TO = 'rotational speed range to'
    RUN_OUT_LENGTH = 'run-out length'
    SAMPLE_PIPELINE_DIAMETER = 'sample pipeline diameter'
    SAMPLE_PIPELINE_LENGTH = 'sample pipeline length'
    SAMPLE_PIPELINE_MATERIAL = 'sample pipeline material'
    SAMPLE_PIPELINE_TYPE = 'sample pipeline type'
    SAMPLE_PREPARATION_BACK_PRESSURE_REGULATOR = 'sample preparation back pressure regulator'
    SAMPLE_PREPARATION_CHEMICAL_TEMPLATE = 'sample preparation chemical template'
    SAMPLE_PREPARATION_FILTER = 'sample preparation filter'
    SAMPLE_PREPARATION_FLOW_COUNTER = 'sample preparation flow counter'
    SAMPLE_PREPARATION_MEASURING_PRODUCT_COOLER = 'sample preparation measuring product cooler'
    SAMPLE_PREPARATION_MISCELLANEOUS = 'sample preparation miscellaneous'
    SAMPLE_PREPARATION_OUTPUT_PRESSURE_MAXIMUM = 'sample preparation output pressure maximum'
    SAMPLE_PREPARATION_OUTPUT_PRESSURE_NORMAL = 'sample preparation output pressure normal'
    SAMPLE_PREPARATION_REDUCTION_STATION = 'sample preparation reduction station'
    SAMPLE_PREPARATION_RETENTION_TIME = 'sample preparation retention time'
    SAMPLE_PREPARATION_SEPARATOR = 'sample preparation separator'
    SAMPLE_PREPARATION_TUBE_FURNACE = 'sample preparation tube furnace'
    SAMPLE_PREPARATION_VAPORIZER = 'sample preparation vaporizer'
    SAMPLE_PROBE_DELAY_TIME = 'sample probe delay time'
    SAMPLE_SENSOR = 'sample sensor'
    SAMPLE_TYPE = 'sample type'
    SCALE_CLASSIFICATION = 'scale classification'
    SCREWED_JOINT = 'screwed joint'
    SEAT_DIAMETER = 'seat diameter'
    SEAT_MATERIAL = 'seat material'
    SEAT_SEALING = 'seat sealing'
    SELF_RETAINING_DRIVE = 'self-retaining drive'
    SERIAL_NUMBER = 'serial number'
    SETTING_RANGE_MAXIMUM = 'setting range maximum'
    SETTING_RANGE_MINIMUM = 'setting range minimum'
    SHORT_CIRCUIT_PROOF = 'short-circuit-proof'
    SIGNAL_DIRECTION = 'signal direction'
    SIGNAL_RANGE = 'signal range'
    SIGNAL_RANGE_RESISTANCE = 'signal range/resistance'
    SIGNAL_TYPE = 'signal type'
    SIGNAL_VOLTAGE_INPUT = 'signal voltage input'
    SIGNAL_VOLTAGE_OUTPUT = 'signal voltage output'
    SIZE = 'size'
    SOLENOID_VALVE = 'solenoid valve'
    SOLENOID_VALVE_DESIGN = 'solenoid valve design'
    SOUND_PRESSURE_LEVEL_AT_NO_LOAD = 'sound pressure level at no load'
    SOUND_PRESSURE_LEVEL_AT_NOMINAL_LOAD = 'sound pressure level at nominal load'
    SPECIAL_PAINT = 'special paint'
    SPECIAL_REQUIREMENTS = 'special requirements'
    SPECIALREMARKS = 'specialremarks'
    SPINDLE_BUSHING = 'spindle bushing'
    SPINDLE_MATERIAL = 'spindle material'
    STANDARD_ORIFICE_NOZZLE_MATERIAL = 'standard orifice/nozzle material'
    STARTING_CURRENT_RATIO_IA_PER_IN = 'starting current ratio ia per in'
    STARTING_TORQUE_RATIO_MA_PER_MN = 'starting torque ratio ma per mn'
    STATUS = 'status'
    STELLITING = 'stelliting'
    STROKE_HEIGHT = 'stroke/height'
    SUFFIX = 'suffix'
    SUPPL_CHAR_3_FOR_LOOP_ELEMENTS = 'suppl char 3 for loop elements'
    SUPPL_CHAR_FOR_INSTR_ELEMENTS_1 = 'suppl. char. for instr. elements 1'
    SUPPL_CHAR_FOR_INSTR_ELEMENTS_2 = 'suppl. char. for instr. elements 2'
    SWITCH_STATE_AT_REQUIRED_END_POSITION = 'switch state at required end position'
    SYMBOL_LIBRARY = 'symbol library'
    SYMBOL_NAME = 'symbol name'
    TAGNO = 'tagno'
    TANDEM_SWITCH = 'tandem switch'
    TAPPING_MATERIAL = 'tapping material'
    TEMPERATURE_CONTROLLER = 'temperature controller'
    TEMPERATURE_LIMITER = 'temperature limiter'
    TEMPLATE_PROTECTION_FLUID = 'template protection fluid'
    TERMINAL_BOX_POSITION_TO_DRIVE_END_OF_MOTOR = 'terminal box position (to drive end of motor)'
    THEORETICAL_RATIO_KVS_KVO = 'theoretical ratio kvs/kvo'
    THERMOCOUPLE = 'thermocouple'
    THERMOMETER_DESIGN = 'thermometer design'
    TMU = 'tmu'
    TORQUE_DEPENDENT = 'torque dependent'
    TORQUE_LIMIT_SWITCH_CLOSE = 'torque limit switch close'
    TORQUE_LIMIT_SWITCH_OPEN = 'torque limit switch open'
    TOTAL_DIMENSIONS_HEIGHT = 'total dimensions height'
    TOTAL_DIMENSIONS_LENGTH = 'total dimensions length'
    TOTAL_DIMENSIONS_WIDTH = 'total dimensions width'
    TOTAL_WEIGHT = 'total weight'
    TOTAL_WEIGHT_OF_MOTOR = 'total weight of motor'
    TRANSFER_TO_SAP = 'transfer to sap'
    TRANSMITTER_PROTECTIVE_BOX = 'transmitter protective box'
    TRAVEL_RANGE = 'travel range'
    TYPE_OF_COOLING = 'type of cooling'
    TYPE_OF_DRIVE = 'type of drive'
    TYPE_OF_STARTING = 'type of starting'
    VALVE_NUMBER = 'valve number'
    VALVE_SIZING_FACTOR_B = 'valve sizing factor b'
    VARIANTS_IN_PARTS = 'variants in parts'
    VOLTAGE = 'voltage'
    VOLTAGE_TYPE = 'voltage type'
    WEIGHT_FOR_CHOKE = 'weight for choke'
    WEIGHT_FOR_CONVERTER = 'weight for converter'
    WETTED_BODY_MATERIAL = 'wetted body material'
    WINDING_CIRCUIT = 'winding circuit'
    WIRING_CONFIG = 'wiring_config'
    YEAR_OF_CONSTRUCTION = 'year of construction'
    ZERO_ELEVATION = 'zero elevation'
