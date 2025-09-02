import pandas as pd
import traceback
import logging

class PlotDataMaker:
    def __init__(self, expenses, transport, budget, industry, market_averages=None):
        self.budget = budget
        self.market_averages = market_averages
        self.expenses = expenses.copy()
        self.transport = transport.copy()
        self.industry = industry

        self.expenses['date'] = pd.to_datetime(self.expenses['date'], format='%Y-%m-%d')
        self.expenses['month'] = self.expenses['date'].dt.to_period('M')
        self.expenses['year'] = self.expenses['date'].dt.to_period('Y')

        if self.market_averages is None:
            self.market_averages = {
                'Логистика': {
                    'all_expenses': 25605.443195501226,
                    'expenses_structure': {
                        'fuel': 11784.783393025677,
                        'repair': 1990.0962618047165,
                        'insurance': 17.51733211904805,
                        'fines': 243.07818560530916,
                        'maintenance': 4.9202608308837235,
                        'tire_service': 375.4417949781489,
                        'carwash': 21.337011896389004,
                        'parking': 467.32370753758573,
                        'antifreeze_liquid': 3.4442608188610966,
                        'flat_roads': 252.00452736049246,
                        'daily_distance': 113.0785448413915,
                        'rental_expense': 2655.6114084507044,
                        'amortization': 1284.3130700378108,
                        'transport_tax': 5.133060419709894,
                        'casco': 174.41825515620397,
                        'trailer': 0.5211267605633803,
                        'engine_oil': 7.689467577981762,
                        'brake_fluid': 3.803566977132262,
                        'spark_plugs': 6.29696079942811,
                        'filters': 15.879936039626577,
                        'fuel_filters': 15.65431942916569,
                        'timing_belts': 0.12576814822520174,
                        'brake_pads': 3471.401862304858,
                        'other_cost': 523.2015343591063,
                        'fire_extinguishers': 5.6769880915883695,
                        'first_aid_kits': 2.8426478632786907,
                        'calculated_salary': 1866.3418403635644,
                        'temp_control_maintenance': 63.65966114226975,
                        'customs_fees': 91.151455399061,
                        'cargo_insurance': 73.18920638641926,
                        'transloading_fees': 65.50577897603289}
                    },
                'Строительство': {
                    'all_expenses': 23853.595043416022,
                    'expenses_structure': {
                        'fuel': 12341.074572485417,
                        'repair': 2103.917988463379,
                        'insurance': 17.970194260143042,
                        'fines': 255.66072194415344,
                        'maintenance': 6.412209159456278,
                        'tire_service': 401.520500523804,
                        'carwash': 21.88702763874339,
                        'parking': 417.78826342813545,
                        'antifreeze_liquid': 3.4310121703591614,
                        'flat_roads': 0.0,
                        'daily_distance': 113.051908069168,
                        'rental_expense': 1854.298787878788,
                        'amortization': 547.7449822165315,
                        'transport_tax': 2.5026190199045515,
                        'casco': 43.45633026811004,
                        'trailer': 0.24242424242424243,
                        'engine_oil': 7.472859193729873,
                        'brake_fluid': 3.9579781692017315,
                        'spark_plugs': 5.952117820599923,
                        'filters': 15.929706799105006,
                        'fuel_filters': 15.775436826653216,
                        'timing_belts': 0.0,
                        'brake_pads': 3778.3532249511763,
                        'other_cost': 527.0162271892686,
                        'fire_extinguishers': 6.009415538224757,
                        'first_aid_kits': 2.8054682548920704,
                        'calculated_salary': 1316.9671357622317,
                        'bucket_parts': 0.0,
                        'special_lubricants': 23.121225054643748}
                    },
                'Фармацевтика': {
                    'all_expenses': 22119.44469605468,
                        'expenses_structure': {
                        'fuel': 9507.485147711037,
                        'repair': 2158.9297855777622,
                        'insurance': 13.634614114197703,
                        'fines': 228.98695615060592,
                        'maintenance': 3.02892482695912,
                        'tire_service': 314.10421134183224,
                        'carwash': 19.53046827738499,
                        'parking': 423.39384002301034,
                        'antifreeze_liquid': 3.4534678691384144,
                        'flat_roads': 175.1997618252333,
                        'daily_distance': 112.79994052589582,
                        'rental_expense': 2765.3293478260866,
                        'amortization': 459.94789966412446,
                        'transport_tax': 2.4986546419491917,
                        'casco': 39.61847501345358,
                        'trailer': 0.0,
                        'engine_oil': 7.621091502904117,
                        'brake_fluid': 4.028432422869712,
                        'spark_plugs': 6.511651441498018,
                        'filters': 15.629200764534506,
                        'fuel_filters': 15.558945239288166,
                        'timing_belts': 0.0,
                        'brake_pads': 3461.8961012451523,
                        'other_cost': 532.7829176886469,
                        'fire_extinguishers': 5.762261314925124,
                        'first_aid_kits': 2.8850600308040604,
                        'calculated_salary': 1523.3967461819668,
                        'temp_control_maintenance': 186.74809562248325,
                        'sterilization_costs': 93.84532158696578,
                        'pharma_licenses': 34.83737562396779}
                    },
                'Ритейл': {
                    'all_expenses': 19141.65097253213,
                    'expenses_structure': {
                        'fuel': 8607.90120913818,
                        'repair': 1839.991029330102,
                        'insurance': 15.42477767222429,
                        'fines': 234.96450956635866,
                        'maintenance': 0.0,
                        'tire_service': 289.59278450107615,
                        'carwash': 18.926995213378675,
                        'parking': 372.7092372154072,
                        'antifreeze_liquid': 3.427241979082052,
                        'flat_roads': 154.85340951541764,
                        'daily_distance': 113.86997856805218,
                        'rental_expense': 1560.146559139785,
                        'amortization': 542.1705658126011,
                        'transport_tax': 2.912817406229492,
                        'casco': 62.72401433691756,
                        'trailer': 0.0,
                        'engine_oil': 7.723032230528823,
                        'brake_fluid': 3.8030649001095322,
                        'spark_plugs': 6.272988543030714,
                        'filters': 15.918026241515564,
                        'fuel_filters': 15.807966076026048,
                        'timing_belts': 0.0,
                        'brake_pads': 3311.318647630324,
                        'other_cost': 546.0777586155605,
                        'fire_extinguishers': 5.6922519148779935,
                        'first_aid_kits': 2.9986874652935533,
                        'calculated_salary': 1406.4234195200527}
                    },
                'Такси': {
                    'all_expenses': 18539.185293844726,
                    'expenses_structure': {
                        'fuel': 8456.119833888177,
                        'repair': 1885.476009048229,
                        'insurance': 17.68253708920188,
                        'fines': 237.57293384549726,
                        'maintenance': 0.0,
                        'tire_service': 263.1655412718737,
                        'carwash': 19.138205377720872,
                        'parking': 350.028563551003,
                        'antifreeze_liquid': 3.4801514297908662,
                        'flat_roads': 0.0,
                        'daily_distance': 113.14272283397354,
                        'rental_expense': 879.2352000000001,
                        'amortization': 1344.4421970123772,
                        'transport_tax': 3.2129748186086213,
                        'casco': 63.93512590695689,
                        'trailer': 0.0,
                        'engine_oil': 7.5459154929577466,
                        'brake_fluid': 3.7984411562497655,
                        'spark_plugs': 5.873636529566063,
                        'filters': 15.795049082373026,
                        'fuel_filters': 15.881912078531796,
                        'timing_belts': 3.3441630148284167,
                        'brake_pads': 3136.341340162185,
                        'other_cost': 539.5613766609409,
                        'fire_extinguishers': 5.84790439607341,
                        'first_aid_kits': 2.8500384122919336,
                        'calculated_salary': 1165.7135207853178}
                    },
                'Доставка': {
                    'all_expenses': 19781.530316567627,
                    'expenses_structure': {
                        'fuel': 8740.611579719185,
                        'repair': 1963.3216613589852,
                        'insurance': 15.736805870186151,
                        'fines': 219.67750068397956,
                        'maintenance': 0.0,
                        'tire_service': 266.27693073749407,
                        'carwash': 18.95934928920844,
                        'parking': 370.03803637676873,
                        'antifreeze_liquid': 3.4585846547818377,
                        'flat_roads': 0.0,
                        'daily_distance': 113.70139619379054,
                        'rental_expense': 2559.2366666666667,
                        'amortization': 308.7912212044606,
                        'transport_tax': 0.8306247742867461,
                        'casco': 13.21448504547096,
                        'trailer': 0.0,
                        'engine_oil': 7.648248465149874,
                        'brake_fluid': 3.7709350546831963,
                        'spark_plugs': 6.474838262422125,
                        'filters': 15.74415881458135,
                        'fuel_filters': 16.03527145780667,
                        'timing_belts': 0.0,
                        'brake_pads': 3287.701095461659,
                        'other_cost': 541.1875166464482,
                        'fire_extinguishers': 5.751195596266019,
                        'first_aid_kits': 2.8466025367433816,
                        'calculated_salary': 1300.5156116965975}
                    },
                'Автобусы': {
                    'all_expenses': 24268.662149859232,
                    'expenses_structure': {
                        'fuel': 10995.86672548945,
                        'repair': 1837.3279332581142,
                        'insurance': 16.922560439142504,
                        'fines': 241.79151253153486,
                        'maintenance': 5.097021547225146,
                        'tire_service': 309.786130162312,
                        'carwash': 25.534322291384072,
                        'parking': 505.76517720786154,
                        'antifreeze_liquid': 3.4409697730336415,
                        'flat_roads': 272.96753920462464,
                        'daily_distance': 113.2089286393428,
                        'rental_expense': 1760.6943564356432,
                        'amortization': 2722.7670152085634,
                        'transport_tax': 6.8795611955561755,
                        'casco': 211.20421901345065,
                        'trailer': 0.0,
                        'engine_oil': 7.652434257510258,
                        'brake_fluid': 3.8262106697252207,
                        'spark_plugs': 6.3348526213472445,
                        'filters': 15.709144153852005,
                        'fuel_filters': 15.641341598948626,
                        'timing_belts': 0.0,
                        'brake_pads': 3585.966574122201,
                        'other_cost': 532.8068699211465,
                        'fire_extinguishers': 5.691628317761354,
                        'first_aid_kits': 2.828015195885786,
                        'calculated_salary': 1062.9511066036182}
                    },
                'Каршеринг': {
                    'all_expenses': 17659.767930180187,
                    'expenses_structure': {
                        'fuel': 8595.443782586428,
                        'repair': 1923.9232755441747,
                        'insurance': 17.0188139991464,
                        'fines': 233.08227827571488,
                        'maintenance': -1.0,
                        'tire_service': 272.4352344003414,
                        'carwash': 18.907465727699535,
                        'parking': 350.02215168587276,
                        'antifreeze_liquid': 3.4619933418693987,
                        'flat_roads': -1.0,
                        'daily_distance': 113.30358924455822,
                        'rental_expense': 246.385,
                        'amortization': 2081.5252408877504,
                        'transport_tax': 4.880921895006402,
                        'casco': 86.64105847204439,
                        'trailer': -1.0,
                        'engine_oil': 7.779026888604354,
                        'brake_fluid': 3.947314067372843,
                        'spark_plugs': 6.179068351577732,
                        'filters': 15.749995731967562,
                        'fuel_filters': 15.249918907383696,
                        'timing_belts': -1.0,
                        'brake_pads': 3127.1941271873666,
                        'other_cost': 527.7651164338777,
                        'fire_extinguishers': 5.933324797268459,
                        'first_aid_kits': 2.9392317541613315,
                        'calculated_salary': -1.0}
                    },
                'Другое': {
                    'all_expenses': 19141.65097253213,
                    'expenses_structure': {
                        'fuel': 8607.90120913818,
                        'repair': 1839.991029330102,
                        'insurance': 15.42477767222429,
                        'fines': 234.96450956635866,
                        'maintenance': -1.0,
                        'tire_service': 289.59278450107615,
                        'carwash': 18.926995213378675,
                        'parking': 372.7092372154072,
                        'antifreeze_liquid': 3.427241979082052,
                        'flat_roads': 154.85340951541764,
                        'daily_distance': 113.86997856805218,
                        'rental_expense': 1560.146559139785,
                        'amortization': 542.1705658126011,
                        'transport_tax': 2.912817406229492,
                        'casco': 62.72401433691756,
                        'trailer': -1.0,
                        'engine_oil': 7.723032230528823,
                        'brake_fluid': 3.8030649001095322,
                        'spark_plugs': 6.272988543030714,
                        'filters': 15.918026241515564,
                        'fuel_filters': 15.807966076026048,
                        'timing_belts': -1.0,
                        'brake_pads': 3311.318647630324,
                        'other_cost': 546.0777586155605,
                        'fire_extinguishers': 5.6922519148779935,
                        'first_aid_kits': 2.9986874652935533,
                        'calculated_salary': 1406.4234195200527}
                    }
                }

            self.market_averages = self.market_averages[industry]

    def salary_data(self):
        try:
            print(18)
            grouped = self.expenses.groupby(by=['month', 'driver'])['calculated_salary'].sum().reset_index()

            salary_pivot = grouped.pivot(index='month', columns='driver', values='calculated_salary').fillna(0)

            result = {}

            for month_period in salary_pivot.index:
                year = month_period.year
                month_str = f"{month_period.month:02d}"

                month_data = salary_pivot.loc[month_period].to_dict()

                if year not in result:
                    result[year] = {}
                result[year][month_str] = month_data
        except TypeError as e:
            if "float division" in str(e) and "by zero" in str(e):
                # Специфичная обработка ошибки DataFrame
                error_msg = (
                    f"Solution: Check dictionary/set operations with DataFrames\n"
                    f"Full error: {traceback.format_exc()}"
                )
                logging.error(error_msg)
                return False
            else:
                raise  # Перебрасываем другие TypeError

        return result


    def fuel_km_per_month(self):
        try:
            print(19)
            expenses_data = self.expenses.groupby(by=['month'])[['fuel', 'daily_distance']].sum().reset_index()
            expenses_data['fuel_effectiveness'] = (expenses_data['fuel'] / expenses_data['daily_distance'].replace(0, -1000)).fillna(-1000)
            res = dict()
            for i in range(expenses_data.shape[0]):
                res[expenses_data.iloc[i]['month'].strftime("%Y")] = {}
            for i in range(expenses_data.shape[0]):
                res[expenses_data.iloc[i]['month'].strftime("%Y")][expenses_data.iloc[i]['month'].strftime("%m")] = {
                        "fuel_in_month": float(expenses_data.iloc[i]['fuel']),
                        "distance_in_month": float(expenses_data.iloc[i]['daily_distance']),
                        "fuel_effectiveness_in_month": float(expenses_data.iloc[i]['fuel_effectiveness'])
                }
        except TypeError as e:
            if "float division" in str(e) and "by zero" in str(e):
                # Специфичная обработка ошибки DataFrame
                error_msg = (
                    f"Solution: Check dictionary/set operations with DataFrames\n"
                    f"Full error: {traceback.format_exc()}"
                )
                logging.error(error_msg)
                return False
            else:
                raise  # Перебрасываем другие TypeError

        return res

    def logistic_expenses_by_month(self):
        try:
            print(20)
            grouped = self.expenses.groupby(by=['month'])[['cargo_insurance', 'temp_control_maintenance', 'customs_fees', 'transloading_fees']].sum().reset_index()

            result = {}
            for _, row in grouped.iterrows():
                year = row['month'].year
                month = f"{row['month'].month:02d}"

                if year not in result:
                    result[year] = {}

                result[year][month] = {
                    "cargo_insurance": float(row['cargo_insurance']),
                    "temp_control_maintenance": float(row['temp_control_maintenance']),
                    "customs_fees": float(row['customs_fees']),
                    "transloading_fees": float(row['transloading_fees']),
                    }
        except TypeError as e:
            if "float division" in str(e) and "by zero" in str(e):
                # Специфичная обработка ошибки DataFrame
                error_msg = (
                    f"Solution: Check dictionary/set operations with DataFrames\n"
                    f"Full error: {traceback.format_exc()}"
                )
                logging.error(error_msg)
                return False
            else:
                raise  # Перебрасываем другие TypeError

        return result

    def pharma_expenses_by_month(self):
        try:
            print(21)
            grouped = self.expenses.groupby(by=['month'])[[
                'sterilization_costs',
                'pharma_licenses',
                'temp_control_maintenance'
            ]].sum().reset_index()

            result = {}
            for _, row in grouped.iterrows():
                year = row['month'].year
                month = f"{row['month'].month:02d}"

                if year not in result:
                    result[year] = {}

                result[year][month] = {
                    "sterialization_cost": float(row['sterilization_costs']),
                    "pharma_licenses_cost": float(row['pharma_licenses']),
                    "temperature_control_maintance": float(row['temp_control_maintenance'])
                }
        except TypeError as e:
            if "float division" in str(e) and "by zero" in str(e):
                # Специфичная обработка ошибки DataFrame
                error_msg = (
                    f"Solution: Check dictionary/set operations with DataFrames\n"
                    f"Full error: {traceback.format_exc()}"
                )
                logging.error(error_msg)
                return False
            else:
                raise  # Перебрасываем другие TypeError

        return result

    def building_expenses_by_month(self):
        try:
            print(22)
            grouped = self.expenses.groupby(['month'])[
                ['bucket_parts', 'special_lubricants']
            ].sum().reset_index()

            result = {}
            for _, row in grouped.iterrows():
                year = row['month'].year
                month = f"{row['month'].month:02d}"

                if year not in result:
                    result[year] = {}

                result[year][month] = {
                    'bucket_parts': float(row['bucket_parts']),
                    'special_lubricants': float(row['special_lubricants'])
                }
        except TypeError as e:
            if "float division" in str(e) and "by zero" in str(e):
                # Специфичная обработка ошибки DataFrame
                error_msg = (
                    f"Solution: Check dictionary/set operations with DataFrames\n"
                    f"Full error: {traceback.format_exc()}"
                )
                logging.error(error_msg)
                return False
            else:
                raise  # Перебрасываем другие TypeError

        return result

    def vehicle_usage_by_month(self):
        try:
            print(23)
            used_data = self.expenses[self.expenses['daily_distance'] > 0].copy()

            grouped = used_data.groupby(['month', 'gov_number']).agg(
                days_used=('daily_distance', 'count'),
                total_distance=('daily_distance', 'sum')
            ).reset_index()

            result = {}
            for _, row in grouped.iterrows():
                year_val = row['month'].year
                month_val = f"{row['month'].month:02d}"
                gov_number = row['gov_number']

                if year_val not in result:
                    result[year_val] = {}
                if month_val not in result[year_val]:
                    result[year_val][month_val] = {}

                result[year_val][month_val][gov_number] = {
                    "days_used": int(row['days_used']),
                    "total_distance": float(row['total_distance'])
                }
        except TypeError as e:
            if "float division" in str(e) and "by zero" in str(e):
                # Специфичная обработка ошибки DataFrame
                error_msg = (
                    f"Solution: Check dictionary/set operations with DataFrames\n"
                    f"Full error: {traceback.format_exc()}"
                )
                logging.error(error_msg)
                return False
            else:
                raise  # Перебрасываем другие TypeError

        return result



    def downtime_counter(self):
        try:
            print(24)
            expenses_data = self.expenses[self.expenses['daily_distance'] == 0].copy()

            if expenses_data.empty:
                return {}

            # Группируем данные без использования concat
            numeric_cols = expenses_data.select_dtypes(include=['int64', 'float64']).columns
            expenses_data['total_expense'] = expenses_data[numeric_cols].sum(axis=1)

            grouped = expenses_data.groupby(['month', 'gov_number']).agg(
                sum=('total_expense', 'sum'),
                count=('total_expense', 'count')
            ).reset_index()

            result = {}
            for _, row in grouped.iterrows():
                month_period = row['month']
                year_val = month_period.year
                month_val = f"{month_period.month:02d}"
                gov_number = row['gov_number']

                if year_val not in result:
                    result[year_val] = {}
                if month_val not in result[year_val]:
                    result[year_val][month_val] = {}

                result[year_val][month_val][gov_number] = {
                    'sum': row['sum'],
                    'count': row['count']
                }

        except TypeError as e:
            if "float division" in str(e) and "by zero" in str(e):
                # Специфичная обработка ошибки DataFrame
                error_msg = (
                    f"Solution: Check dictionary/set operations with DataFrames\n"
                    f"Full error: {traceback.format_exc()}"
                )
                logging.error(error_msg)
                return False
            else:
                raise  # Перебрасываем другие TypeError

        return result

    def downtime_finder(self):
        try:
            print(25)
            filtered = self.expenses[self.expenses['daily_distance'] == 0].copy()

            numeric_cols = filtered.select_dtypes(include=['int64','float64']).columns
            filtered['total_expense'] = filtered[numeric_cols].sum(axis=1)

            grouped = filtered.groupby(['year', 'month', 'gov_number'])['total_expense'].agg(['sum', 'count']).reset_index()

            result = {}
            for index, row in grouped.iterrows():
                year_val = row['year'].year
                month_val = row['month'].strftime("%m")

                if year_val not in result:
                    result[year_val] = {}
                if month_val not in result[year_val]:
                    result[year_val][month_val] = {}

                result[year_val][month_val][row['gov_number']] = {
                    'sum': row['sum'],
                    'count': row['count']
                }
        except TypeError as e:
            if "float division" in str(e) and "by zero" in str(e):
                # Специфичная обработка ошибки DataFrame
                error_msg = (
                    f"Solution: Check dictionary/set operations with DataFrames\n"
                    f"Full error: {traceback.format_exc()}"
                )
                logging.error(error_msg)
                return False
            else:
                raise  # Перебрасываем другие TypeError

        return result

    def aggregate_expenses_by_month_year(self):
        try:
            print(26)
            df = self.expenses.copy()

            numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()

            non_bool_cols = []
            for col in numeric_cols:
                unique_vals = df[col].dropna().unique()
                if set(unique_vals).issubset({0, 1}):
                    continue
                non_bool_cols.append(col)

            if 'daily_distance' in non_bool_cols:
                non_bool_cols.remove('daily_distance')

            non_bool_cols.extend(['year', 'month'])

            grouped = df[non_bool_cols].groupby(['year', 'month']).sum()

            result = {}
            for (year_period, month_period), row in grouped.iterrows():
                year = year_period.year
                month = f"{month_period.month:02d}"

                if year not in result:
                    result[year] = {}

                month_expenses = {}
                for col in non_bool_cols:
                    if col in ['year', 'month']:
                        continue
                    month_expenses[col] = float(row[col])

                result[year][month] = month_expenses
        except TypeError as e:
            if "float division" in str(e) and "by zero" in str(e):
                # Специфичная обработка ошибки DataFrame
                error_msg = (
                    f"Solution: Check dictionary/set operations with DataFrames\n"
                    f"Full error: {traceback.format_exc()}"
                )
                logging.error(error_msg)
                return False
            else:
                raise  # Перебрасываем другие TypeError

        return result

    def vehicle_efficiency_per_year(self):
        try:
            print(27)
            df = self.expenses.copy()

            numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()

            expense_cols = []
            for col in numeric_cols:
                unique_vals = df[col].dropna().unique()
                if set(unique_vals).issubset({0, 1}):
                    continue
                if col != 'daily_distance':
                    expense_cols.append(col)

            df['total_expenses'] = df[expense_cols].sum(axis=1)

            grouped = df.groupby(['year', 'gov_number']).apply(
                lambda x: pd.Series({
                    'total_distance': x['daily_distance'].sum(),
                    'total_expenses': x['total_expenses'].sum()
                })
            ).reset_index()

            grouped['efficiency'] = grouped['total_distance'] / grouped['total_expenses'] * 100

            result = {}
            for _, row in grouped.iterrows():
                year = row['year'].year
                gov_number = row['gov_number']
                efficiency = row['efficiency']

                if year not in result:
                    result[year] = {}

                result[year][gov_number] = float(efficiency)
        except TypeError as e:
            if "float division" in str(e) and "by zero" in str(e):
                # Специфичная обработка ошибки DataFrame
                error_msg = (
                    f"Solution: Check dictionary/set operations with DataFrames\n"
                    f"Full error: {traceback.format_exc()}"
                )
                logging.error(error_msg)
                return False
            else:
                raise  # Перебрасываем другие TypeError

        return result

    def amortization_by_month_year(self):
        try:
            print(28)
            amort_data = self.expenses.groupby(
                ['year', 'month']
            )['amortization'].sum().reset_index()

            result = {}
            for _, row in amort_data.iterrows():
                year = row['year'].year
                month = f"{row['month'].month:02d}"
                amort_value = float(row['amortization'])

                if year not in result:
                    result[year] = {}
                result[year][month] = {"amortization": amort_value}
        except TypeError as e:
            if "float division" in str(e) and "by zero" in str(e):
                # Специфичная обработка ошибки DataFrame
                error_msg = (
                    f"Solution: Check dictionary/set operations with DataFrames\n"
                    f"Full error: {traceback.format_exc()}"
                )
                logging.error(error_msg)
                return False
            else:
                raise  # Перебрасываем другие TypeError

        return result

    def repair_expenses_by_month_year(self):
        try:
            print(29)
            df = self.expenses.copy()

            numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()

            non_repair_columns = [
                'daily_distance', 'amortization', 'transport_tax', 'casco',
                'calculated_salary', 'fuel', 'fines', 'rental_expense', 'insurance',
                'parking', 'carwash', 'flat_roads', 'fire_extinguishers','first_aid_kits',
                'temp_control_maintenance', 'customs_fees', 'transloading_fees,',
                'sterilization_costs', 'pharma_licenses',
            ]

            repair_cols = [col for col in numeric_cols if col not in non_repair_columns]

            final_repair_cols = []
            for col in repair_cols:
                unique_vals = df[col].nunique()
                if unique_vals > 2:
                    final_repair_cols.append(col)

            final_repair_cols.extend(['year', 'month'])

            grouped = df[final_repair_cols].groupby(['year', 'month']).sum()

            result = {}
            for (year_period, month_period), row in grouped.iterrows():
                year = year_period.year
                month = f"{month_period.month:02d}"

                if year not in result:
                    result[year] = {}

                total_repair = row.drop(['year', 'month'], errors='ignore').sum()

                structure = {}
                for col in final_repair_cols:
                    if col in ['year', 'month']:
                        continue
                    structure[col] = float(row[col])

                result[year][month] = {
                    'total': float(total_repair),
                    'structure': structure
                }
        except TypeError as e:
            if "float division" in str(e) and "by zero" in str(e):
                # Специфичная обработка ошибки DataFrame
                error_msg = (
                    f"Solution: Check dictionary/set operations with DataFrames\n"
                    f"Full error: {traceback.format_exc()}"
                )
                logging.error(error_msg)
                return False
            else:
                raise  # Перебрасываем другие TypeError

        return result

    def _normalize_series(self, s: pd.Series):
        return (s - s.min()) / (s.max() - s.min() + 1e-9)

    def top_drivers_fines_by_month(self, top_n=None):
        try:
            print(30)
            all_fines = self.expenses.copy()
            all_fines['month'] = all_fines['date'].dt.month.astype(str).str.zfill(2)
            all_fines['year'] = all_fines['date'].dt.year.astype(str)
            grouped = (
                all_fines
                .groupby(['year', 'month', 'driver'])['fines']
                .agg(sum='sum', count_nonzero=lambda x: (x > 0).sum())
                .reset_index()
                .rename(columns={'count_nonzero': 'count'})
            )
            result = {}
            for (year, month), group in grouped.groupby(['year', 'month']):
                sums = group.set_index('driver')['sum'].to_dict()
                counts = group.set_index('driver')['count'].to_dict()
                sorted_drivers = sorted(sums.items(), key=lambda x: x[1], reverse=True)
                if top_n:
                    sorted_drivers = sorted_drivers[:top_n]
                month_stats = {
                    driver: {"sum": sums[driver], "count": counts[driver]}
                    for driver, _ in sorted_drivers
                }
                result.setdefault(year, {})[month] = month_stats
        except TypeError as e:
            if "float division" in str(e) and "by zero" in str(e):
                # Специфичная обработка ошибки DataFrame
                error_msg = (
                    f"Solution: Check dictionary/set operations with DataFrames\n"
                    f"Full error: {traceback.format_exc()}"
                )
                logging.error(error_msg)
                return False
            else:
                raise  # Перебрасываем другие TypeError

        return result

    def top_drivers_salary_efficiency_by_month(self, top_n=None):
        try:
            print(31)
            df = self.expenses.copy()
            df['month'] = df['date'].dt.month.astype(str).str.zfill(2)
            df['year'] = df['date'].dt.year.astype(str)
            grouped = (
                df
                .groupby(['year', 'month', 'driver'])
                .agg(
                    total_distance=('daily_distance', 'sum'),
                    total_salary=('calculated_salary', 'sum')
                )
                .reset_index()
            )
            grouped = grouped[grouped['total_salary'] > 0]
            grouped['raw_salary_eff'] = grouped['total_distance'] / grouped['total_salary']
            grouped['salary_eff_norm'] = self._normalize_series(grouped['raw_salary_eff'])
            result = {}
            for (year, month), g in grouped.groupby(['year', 'month']):
                effs = g.set_index('driver')['salary_eff_norm'].to_dict()
                sorted_effs = dict(sorted(effs.items(), key=lambda x: x[1], reverse=True))
                if top_n:
                    sorted_effs = dict(list(sorted_effs.items())[:top_n])
                result.setdefault(year, {})[month] = sorted_effs
        except TypeError as e:
            if "float division" in str(e) and "by zero" in str(e):
                # Специфичная обработка ошибки DataFrame
                error_msg = (
                    f"Solution: Check dictionary/set operations with DataFrames\n"
                    f"Full error: {traceback.format_exc()}"
                )
                logging.error(error_msg)
                return False
            else:
                raise  # Перебрасываем другие TypeError

        return result

    def seasonal_fines(self):
        try:
            print(32)
            df = self.expenses.copy()
            df['month'] = df['date'].dt.month.astype(str).str.zfill(2)
            df['year'] = df['date'].dt.year.astype(str)

            result = {}
            seasons = {
                '1 квартал': ['01', '02', '03'],
                '2 квартал': ['06', '04', '05'],
                '3 квартал': ['09', '07', '08'],
                '4 квартал': ['12', '10', '11']
            }

            for year, group in df.groupby('year'):
                monthly = group.groupby('month')['fines'].sum().to_dict()
                monthly = {month: float(value) for month, value in monthly.items()}
                seasonal_sums = {
                    season: sum(monthly.get(m, 0.0) for m in months)
                    for season, months in seasons.items()
                }
                max_season = max(seasonal_sums, key=seasonal_sums.get)
                result[year] = {
                    "monthly_fines": monthly,
                    "seasonal_sums": seasonal_sums,
                    "max_season": max_season
                }
        except TypeError as e:
            if "float division" in str(e) and "by zero" in str(e):
                # Специфичная обработка ошибки DataFrame
                error_msg = (
                    f"Solution: Check dictionary/set operations with DataFrames\n"
                    f"Full error: {traceback.format_exc()}"
                )
                logging.error(error_msg)
                return False
            else:
                raise  # Перебрасываем другие TypeError

        return result