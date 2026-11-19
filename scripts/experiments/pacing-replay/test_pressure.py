import unittest

from pressure import Account, ShortWindow, budget, plan_pressure


class PressureTests(unittest.TestCase):
    def test_expiring_kimi_abundant_short_window(self):
        # The 19 Nov case as pinned in subscription.rs:832-871.
        account = Account(6.2, 2 / 168, short_windows=(ShortWindow(300, 100, 0.22),))
        result = budget([account], 5)
        self.assertAlmostEqual(result.points, 88.8)
        self.assertAlmostEqual(result.max_pressure, 0.22, places=12)
        self.assertAlmostEqual(result.expiry_pressure, 0.0151, places=4)

    def test_binding_short_window_and_exact_quarter(self):
        def pressure(points):
            return budget([Account(6.2, 2 / 168, short_windows=(
                ShortWindow(300, points, 0.22),))], 5)

        binding = pressure(20)
        self.assertAlmostEqual(binding.max_pressure, 5.5)
        self.assertEqual(binding.expiry_pressure, binding.max_pressure)
        quarter = pressure(25)
        self.assertAlmostEqual(quarter.max_pressure, 3.52)
        self.assertAlmostEqual(quarter.expiry_pressure, 0.0151, places=4)

    def test_pool_by_duration_excluding_short_walled_accounts(self):
        result = budget([
            Account(6.2, 2 / 168, short_windows=(ShortWindow(300, 10, 0.22),)),
            Account(6.2, 2 / 168, short_windows=(ShortWindow(300, 20, 0.10),)),
            Account(6.2, 2 / 168, True, (ShortWindow(300, 0, 1.0),)),
        ], 5)
        self.assertAlmostEqual(result.points, 177.6)
        self.assertAlmostEqual(result.max_pressure, 9.777777777777779)
        self.assertEqual(result.expiry_pressure, result.max_pressure)

    def test_weekly_capacity_and_margin(self):
        accounts = [Account(used, 0.5) for used in [100, 97, 3, 27, 17]]
        self.assertEqual(budget(accounts, 0).points, 256)
        self.assertEqual(budget(accounts, 10).points, 223)
        self.assertEqual(budget(accounts, 20).points, 193)
        walled = budget([Account(100, 0.9), Account(0, 0.5)], 0)
        self.assertEqual(walled.max_pressure, 3.6)

    def test_pressure_clamps(self):
        self.assertEqual(plan_pressure(0, 1), 0.01)
        self.assertEqual(plan_pressure(0.5, 0), 1_000_000.0)
        self.assertEqual(plan_pressure(2, 0.5), 4)


if __name__ == "__main__":
    unittest.main()
