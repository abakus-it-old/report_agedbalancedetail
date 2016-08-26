# -*- coding: utf-8 -*-
from openerp import fields, models
from openerp.tools.translate import _

import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
from openerp.exceptions import UserError

class account_aged_trial_balance_detail(models.TransientModel):
    _inherit = 'account.common.partner.report'
    _name = 'account.aged.balance.detail'
    _description = 'Account aged balance detail report'
    
    def compute_default_company_id(self):
        return self.env['res.users'].browse(self.env.uid).company_id

    period_length = fields.Integer(string='Period Length (days)', default=30, required=True)
    direction_selection = fields.Selection([('past','Past'), ('future','Future')], default="past", string='Analysis Direction', required=True)
    date_from = fields.Date(default=lambda *a: time.strftime('%Y-%m-%d'))
    company_id = fields.Many2one(comodel_name='res.company', string='Company', required=True, default=compute_default_company_id)

    def _print_report(self, data):
        res = {}

        data = self.pre_print_report(data)
        data['form'].update(self.read(['period_length', 'direction_selection', 'company_id'])[0])

        period_length = data['form']['period_length']
        if period_length<=0:
            raise UserError(_('User Error!'), _('You must set a period length greater than 0.'))
        if not data['form']['date_from']:
            raise UserError(_('User Error!'), _('You must set a start date.'))

        start = datetime.strptime(data['form']['date_from'], "%Y-%m-%d")

        if data['form']['direction_selection'] == 'past':
            for i in range(5)[::-1]:
                stop = start - relativedelta(days=period_length)
                res[str(i)] = {
                    'name': (i!=0 and (str((5-(i+1)) * period_length) + '-' + str((5-i) * period_length)) or ('+'+str(4 * period_length))),
                    'stop': start.strftime('%Y-%m-%d'),
                    'start': (i!=0 and stop.strftime('%Y-%m-%d') or False),
                }
                start = stop - relativedelta(days=1)
        else:
            for i in range(5):
                stop = start + relativedelta(days=period_length)
                res[str(5-(i+1))] = {
                    'name': (i!=4 and str((i) * period_length)+'-' + str((i+1) * period_length) or ('+'+str(4 * period_length))),
                    'start': start.strftime('%Y-%m-%d'),
                    'stop': (i!=4 and stop.strftime('%Y-%m-%d') or False),
                }
                start = stop + relativedelta(days=1)
        data['form'].update(res)
        if data.get('form',False):
            data['ids']=[data['form'].get('chart_account_id',False)]

        return self.env['report'].get_action(self, 'report_agedbalancedetail.report_agedbalancedetail', data=data)