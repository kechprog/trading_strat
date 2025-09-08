def on_start(self):
    self.log.info("Strategy Started")
    
    # This list will be appended at event handler and will be exported as dataFrame at the end of the strategy.
    self.account_report_rows = []

    # Settting a countinous one minute timer for the strategy for logging the account report.
    self.clock.set_timer(
        name="equity_curve_timer",
        interval=pd.Timedelta(minutes=1)
        )
    
def on_event(self, event: Event) -> None:
    if isinstance(event, TimeEvent): # Time Event for equity_curve_timer
                    
        if event.name == "equity_curve_timer":
            account = list(self.portfolio.account(Venue("NSE")).balances().values())[0]
            self.account_report_rows.append({
                "Time": self.clock.utc_now(),
                "Total": account.total.raw / 1_000_000_000,
                "Free": account.free.raw / 1_000_000_000,
                "Realized_pnls": (list(self.portfolio.realized_pnls(Venue("NSE")).values())[0].raw / 1_000_000_000
                                    if self.portfolio.realized_pnls(Venue("NSE")).values()
                                    else np.nan),
                "Unrealized_pnls": (list(self.portfolio.unrealized_pnls(Venue("NSE")).values())[0].raw / 1_000_000_000
                                    if self.portfolio.unrealized_pnls(Venue("NSE")).values()
                                    else np.nan)
            })

def on_stop(self): 
    self.log.info("Strategy Stopped")

    self.account_report_df = pd.DataFrame(self.account_report_rows, columns=["Time", "Total", "Free", "Realized_pnls", "Unrealized_pnls"])
    self.account_report_df.to_csv(r'generated_report\equity_curve_for_varient.csv')