class Scenario:
    mortgage_value = 0
    deposit_percent = 0
    deposit_value = 0
    price= 0
    monthly_mortgage = 0
    monthly_rent = 0
    other_monthly_expenses = 0
    stamp_duty_value = 0
    other_purchasing_fees =''
    total_monthly_expenses = 0
    capital_raised = 0
    years = 0
    interest_rate = 0

    annual_roi = 0
    monthly_profit = 0
    capital_to_raise =0
    total_investment = 0


    def __init__(self,
                    mortgage_value,
                    deposit_percent,
                    years,
                    interest_rate,
                    deposit_value,
                    price,
                    monthly_mortgage ,
                    monthly_rent ,
                    other_monthly_expenses ,
                    stamp_duty_value,
                    other_purchasing_fees ,
                    total_monthly_expenses ,
                    capital_raised ):

        self.mortgage_value = mortgage_value
        self.deposit_percent = deposit_percent
        self.years = years
        self.interest_rate = interest_rate
        self.deposit_value = deposit_value
        self.price = price
        self.monthly_mortgage = monthly_mortgage
        self.monthly_rent = monthly_rent
        self.other_monthly_expenses = other_monthly_expenses
        self.stamp_duty_value = stamp_duty_value
        self.other_purchasing_fees = other_purchasing_fees
        self.total_monthly_expenses = total_monthly_expenses
        self.capital_raised = capital_raised


    def calculate_all(self):
        self.calculate_deposit_value()
        self.calculate_stamp_duty()
        self.calculate_total_investment()
        self.calculate_mortgage_value()
        self.calculate_monthly_mortgage()
        self.calculate_total_monthly_expenses()
        self.calculate_monthly_profit()
        self.calulate_annual_roi()
        return 0



    def set_other_monthly_expenses(self, amount):
        self.other_monthly_expenses = amount
        return self.other_monthly_expenses

    def set_other_purchasing_fees(self, amount):
        self.other_purchasing_fees = amount
        return self.other_purchasing_fees

    def calculate_mortgage_value(self):
        if self.deposit_value != '':
            self.mortgage_value = self.price - self.deposit_value
        return self.mortgage_value

    def calculate_monthly_mortgage(self):
        months = self.years * 12
        interestRate = float(self.interest_rate) / 100 / 12
        mortgagePayment = self.mortgage_value * (interestRate * (1 + interestRate)
                                        ** months) / ((1 + interestRate) ** months - 1)
        self.monthly_mortgage = mortgagePayment
        return mortgagePayment

    def calculate_total_monthly_expenses (self):
        self.total_monthly_expenses = self.monthly_mortgage + self.other_monthly_expenses
        return self.total_monthly_expenses

    def calculate_stamp_duty (self):
        sd =0

        if self.price  < 125000: sd = 0
        if self.price  > 250000:
            sd = sd + (125000 * 0.02)
        else:
            sd = sd + (self.price - 125000) * 0.02
        if self.price > 925000:
            sd = sd + (675000* 0.05)
        else:
            if self.price > 250000:
                sd = sd + (self.price - 250000) * 0.05

        if self.price > 1500000:
            sd = sd + (575000 * 0.1)
        else:
            if self.price > 925000:
                sd = sd + (self.price - 925000) * 0.1
        if self.price > 1500000:
            sd = sd + (self.price - 1500000) * 0.12
        self.stamp_duty_value = sd
        return sd


    def calculate_deposit_value (self):
        self.deposit_value = self.deposit_percent * self.price
        return self.deposit_value


    def calculate_monthly_profit (self):
        self.monthly_profit = self.monthly_rent - self.total_monthly_expenses
        return self.monthly_profit

    def calulate_annual_roi (self):
        if self.monthly_profit != 0 and self.total_investment != 0:
            try:
                self.annual_roi = (self.monthly_profit * 12) / self.deposit_value
            except:
                self.annual_roi = -1
        return  self.annual_roi

    def calculate_total_investment (self):
        self.total_investment = self.deposit_value + self.other_purchasing_fees + self.stamp_duty_value
        return  self.total_investment

def run_scenario(complete_rm_sales_results, deposit_percent, other_monthly_expenses, other_purchasing_fees, capital_raised, years, interest_rate):
    s = Scenario(
        mortgage_value=0,
        deposit_percent=deposit_percent,
        deposit_value=0,
        price=0,
        monthly_mortgage=0,
        monthly_rent=0,
        other_monthly_expenses=other_monthly_expenses,
        stamp_duty_value=0,
        other_purchasing_fees=other_purchasing_fees,
        total_monthly_expenses=0,
        capital_raised=capital_raised,
        years=years,
        interest_rate=interest_rate)

    for i, e in enumerate(complete_rm_sales_results):
        try:
            s.price = int(complete_rm_sales_results.loc[i, 'price'])
        except Exception as e:
            s.price = -1
        try:
            s.monthly_rent = int(complete_rm_sales_results.outcode_average_cost_per_room[i])
        except Exception as e:
            s.monthly_rent = -1

        s.calculate_all()
        complete_rm_sales_results.loc[i, 'mortgage_value'] = s.mortgage_value
        complete_rm_sales_results.loc[i, 'deposit_value'] = s.deposit_value
        complete_rm_sales_results.loc[i, 'stamp_duty_value'] = s.stamp_duty_value
        complete_rm_sales_results.loc[i, 'other_purchasing_fees'] = s.other_purchasing_fees
        complete_rm_sales_results.loc[i, 'total_investment'] = s.total_investment
        complete_rm_sales_results.loc[i, 'capital_raised'] = s.capital_raised
        complete_rm_sales_results.loc[i, 'monthly_mortgage'] = s.monthly_mortgage
        complete_rm_sales_results.loc[i, 'monthly_rent'] = s.monthly_rent
        complete_rm_sales_results.loc[i, 'other_monthly_expenses'] = s.other_monthly_expenses
        complete_rm_sales_results.loc[i, 'total_monthly_expenses'] = s.total_monthly_expenses
        complete_rm_sales_results.loc[i, 'monthly_profit'] = s.monthly_profit
        complete_rm_sales_results.loc[i, 'annual_roi'] = s.annual_roi
    print('Scenario analysis complete')
    return complete_rm_sales_results

# a = Scenario(
#     mortgage_value = '',
#     deposit_percent = 0.1,
#     deposit_value = '',
#     price= 400000,
#     monthly_mortgage = '',
#     monthly_rent = '',
#     other_monthly_expenses = '',
#     stamp_duty_value = '',
#     other_purchasing_fees ='',
#     total_monthly_expenses = '',
#     capital_raised = '')
#
#
# print(a.calculate_stamp_duty())