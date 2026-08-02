#include "tinvest/decimal.hpp"

#include <gtest/gtest.h>

using tinvest::Decimal;
using tinvest::Money;

TEST(Decimal, NormalizesNanoOverflow) {
  const Decimal d(1, 1'500'000'000);
  EXPECT_EQ(d.units(), 2);
  EXPECT_EQ(d.nano(), 500'000'000);
}

TEST(Decimal, NormalizesMixedSigns) {
  const Decimal d(1, -500'000'000);  // 1 - 0.5 = 0.5
  EXPECT_EQ(d.units(), 0);
  EXPECT_EQ(d.nano(), 500'000'000);

  const Decimal e(-1, 250'000'000);  // -1 + 0.25 = -0.75
  EXPECT_EQ(e.units(), 0);
  EXPECT_EQ(e.nano(), -750'000'000);
}

TEST(Decimal, Arithmetic) {
  const Decimal a(10, 500'000'000);   // 10.5
  const Decimal b(2, 250'000'000);    // 2.25
  EXPECT_EQ(a + b, Decimal(12, 750'000'000));
  EXPECT_EQ(a - b, Decimal(8, 250'000'000));
  EXPECT_EQ(a * 2, Decimal(21));
  EXPECT_EQ(a * b, Decimal(23, 625'000'000));  // 10.5 * 2.25 = 23.625
  EXPECT_EQ(a / 2, Decimal(5, 250'000'000));
  EXPECT_EQ(Decimal(1) / Decimal(3), Decimal(0, 333'333'333));
}

TEST(Decimal, NegativeArithmetic) {
  const Decimal a(-10, -500'000'000);  // -10.5
  EXPECT_EQ(-a, Decimal(10, 500'000'000));
  EXPECT_EQ(a * 2, Decimal(-21));
  EXPECT_EQ(a + Decimal(10, 500'000'000), Decimal(0));
  EXPECT_EQ(Decimal(-1) / Decimal(2), Decimal(0, -500'000'000));
}

TEST(Decimal, Comparison) {
  EXPECT_LT(Decimal(1, 999'999'999), Decimal(2));
  EXPECT_GT(Decimal(0, 1), Decimal(0, -1));
  EXPECT_LE(Decimal(5), Decimal(5));
  EXPECT_NE(Decimal(5), Decimal(5, 1));
}

TEST(Decimal, ToString) {
  EXPECT_EQ(Decimal(123).to_string(), "123");
  EXPECT_EQ(Decimal(123, 450'000'000).to_string(), "123.45");
  EXPECT_EQ(Decimal(0, -500'000'000).to_string(), "-0.5");
  EXPECT_EQ(Decimal(-7, -10'000'000).to_string(), "-7.01");
  EXPECT_EQ(Decimal(0, 1).to_string(), "0.000000001");
}

TEST(Decimal, FromString) {
  EXPECT_EQ(Decimal::from_string("123.45"), Decimal(123, 450'000'000));
  EXPECT_EQ(Decimal::from_string("-0.5"), Decimal(0, -500'000'000));
  EXPECT_EQ(Decimal::from_string("42"), Decimal(42));
  EXPECT_EQ(Decimal::from_string("0.000000001"), Decimal(0, 1));
  EXPECT_FALSE(Decimal::from_string("abc").has_value());
  EXPECT_FALSE(Decimal::from_string("").has_value());
  EXPECT_FALSE(Decimal::from_string("1.2.3").has_value());
}

TEST(Decimal, FromDouble) {
  EXPECT_EQ(Decimal::from_double(1.5), Decimal(1, 500'000'000));
  EXPECT_EQ(Decimal::from_double(-2.25), Decimal(-2, -250'000'000));
  EXPECT_NEAR(Decimal::from_double(0.1).to_double(), 0.1, 1e-9);
}

TEST(Decimal, QuotationRoundTrip) {
  const Decimal d(314, 159'000'000);
  const auto q = d.to_quotation();
  EXPECT_EQ(q.units(), 314);
  EXPECT_EQ(q.nano(), 159'000'000);
  EXPECT_EQ(Decimal(q), d);
}

TEST(Money, RoundTrip) {
  const Money m("rub", Decimal(99, 990'000'000));
  const auto pb = m.to_pb();
  EXPECT_EQ(pb.currency(), "rub");
  EXPECT_EQ(Money(pb), m);
  EXPECT_EQ(m.to_string(), "99.99 rub");
}
