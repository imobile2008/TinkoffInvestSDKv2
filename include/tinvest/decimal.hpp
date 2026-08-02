#pragma once

#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "tinvest/pb.hpp"

namespace tinvest {

// Fixed-point decimal matching the API's Quotation/MoneyValue representation:
// value = units + nano / 1e9. Integer arithmetic only — no floating-point
// drift on the price path. Invariant: |nano| < 1e9 and sign(units)==sign(nano)
// (or either is zero).
class Decimal {
 public:
  static constexpr std::int64_t kNano = 1'000'000'000;

  constexpr Decimal() = default;
  constexpr Decimal(std::int64_t units, std::int32_t nano = 0)
      : units_(units), nano_(nano) {
    normalize();
  }
  Decimal(const pb::Quotation& q) : Decimal(q.units(), q.nano()) {}
  explicit Decimal(const pb::MoneyValue& m) : Decimal(m.units(), m.nano()) {}

  static Decimal from_double(double v) {
    const double u = v < 0 ? -std::floor(-v) : std::floor(v);
    const auto units = static_cast<std::int64_t>(u);
    const auto nano =
        static_cast<std::int64_t>(std::llround((v - u) * kNano));
    return from_total(static_cast<__int128>(units) * kNano + nano);
  }

  // Parses "-123.456"; up to 9 fractional digits, extra digits are truncated.
  static std::optional<Decimal> from_string(std::string_view s);

  constexpr std::int64_t units() const { return units_; }
  constexpr std::int32_t nano() const { return nano_; }
  constexpr bool is_zero() const { return units_ == 0 && nano_ == 0; }

  double to_double() const {
    return static_cast<double>(units_) + static_cast<double>(nano_) / 1e9;
  }
  std::string to_string() const;

  pb::Quotation to_quotation() const {
    pb::Quotation q;
    q.set_units(units_);
    q.set_nano(nano_);
    return q;
  }
  pb::MoneyValue to_money(std::string currency) const {
    pb::MoneyValue m;
    m.set_currency(std::move(currency));
    m.set_units(units_);
    m.set_nano(nano_);
    return m;
  }

  friend constexpr Decimal operator+(Decimal a, Decimal b) {
    return from_total(a.total() + b.total());
  }
  friend constexpr Decimal operator-(Decimal a, Decimal b) {
    return from_total(a.total() - b.total());
  }
  constexpr Decimal operator-() const { return from_total(-total()); }

  // Rounded half-away-from-zero. Products beyond ~1.7e38 nano² overflow
  // __int128 — irrelevant for real prices/volumes.
  friend constexpr Decimal operator*(Decimal a, Decimal b) {
    return from_total(div_round(a.total() * b.total(), kNano));
  }
  friend constexpr Decimal operator*(Decimal a, std::int64_t k) {
    return from_total(a.total() * k);
  }
  friend constexpr Decimal operator*(std::int64_t k, Decimal a) { return a * k; }
  friend constexpr Decimal operator/(Decimal a, std::int64_t k) {
    return from_total(div_round(a.total(), k));
  }
  friend constexpr Decimal operator/(Decimal a, Decimal b) {
    return from_total(div_round(a.total() * kNano, b.total()));
  }

  Decimal& operator+=(Decimal o) { return *this = *this + o; }
  Decimal& operator-=(Decimal o) { return *this = *this - o; }
  Decimal& operator*=(Decimal o) { return *this = *this * o; }

  friend constexpr bool operator==(Decimal a, Decimal b) {
    return a.units_ == b.units_ && a.nano_ == b.nano_;
  }
  friend constexpr bool operator<(Decimal a, Decimal b) {
    return a.total() < b.total();
  }
  friend constexpr bool operator>(Decimal a, Decimal b) { return b < a; }
  friend constexpr bool operator<=(Decimal a, Decimal b) { return !(b < a); }
  friend constexpr bool operator>=(Decimal a, Decimal b) { return !(a < b); }
  friend constexpr bool operator!=(Decimal a, Decimal b) { return !(a == b); }

 private:
  constexpr __int128 total() const {
    return static_cast<__int128>(units_) * kNano + nano_;
  }
  static constexpr Decimal from_total(__int128 t) {
    Decimal d;
    d.units_ = static_cast<std::int64_t>(t / kNano);
    d.nano_ = static_cast<std::int32_t>(t % kNano);
    return d;
  }
  static constexpr __int128 div_round(__int128 num, __int128 den) {
    const __int128 half = den / 2;
    return (num >= 0) == (den >= 0) ? (num + half) / den : (num - half) / den;
  }
  constexpr void normalize() {
    if (nano_ <= -kNano || nano_ >= kNano) {
      units_ += nano_ / kNano;
      nano_ = static_cast<std::int32_t>(nano_ % kNano);
    }
    if (units_ > 0 && nano_ < 0) {
      units_ -= 1;
      nano_ += kNano;
    } else if (units_ < 0 && nano_ > 0) {
      units_ += 1;
      nano_ -= kNano;
    }
  }

  std::int64_t units_ = 0;
  std::int32_t nano_ = 0;
};

inline std::string Decimal::to_string() const {
  std::string s;
  if (units_ < 0 || nano_ < 0) s += '-';
  const std::uint64_t u =
      units_ < 0 ? static_cast<std::uint64_t>(-(units_ + 1)) + 1
                 : static_cast<std::uint64_t>(units_);
  s += std::to_string(u);
  std::uint32_t frac = nano_ < 0 ? static_cast<std::uint32_t>(-nano_)
                                 : static_cast<std::uint32_t>(nano_);
  if (frac != 0) {
    char buf[10];
    for (int i = 8; i >= 0; --i) {
      buf[i] = static_cast<char>('0' + frac % 10);
      frac /= 10;
    }
    int len = 9;
    while (len > 0 && buf[len - 1] == '0') --len;
    s += '.';
    s.append(buf, static_cast<std::size_t>(len));
  }
  return s;
}

inline std::optional<Decimal> Decimal::from_string(std::string_view s) {
  if (s.empty()) return std::nullopt;
  bool neg = false;
  std::size_t i = 0;
  if (s[0] == '-' || s[0] == '+') {
    neg = s[0] == '-';
    i = 1;
  }
  std::int64_t units = 0;
  bool any = false;
  for (; i < s.size() && s[i] >= '0' && s[i] <= '9'; ++i) {
    units = units * 10 + (s[i] - '0');
    any = true;
  }
  std::int32_t nano = 0;
  if (i < s.size()) {
    if (s[i] != '.' && s[i] != ',') return std::nullopt;
    ++i;
    std::int64_t scale = kNano / 10;
    for (; i < s.size() && s[i] >= '0' && s[i] <= '9'; ++i) {
      nano += static_cast<std::int32_t>((s[i] - '0') * scale);
      scale /= 10;
      any = true;
    }
    if (i != s.size()) return std::nullopt;
  }
  if (!any) return std::nullopt;
  return Decimal(neg ? -units : units, neg ? -nano : nano);
}

// Money = amount + ISO currency code, converting to/from pb::MoneyValue.
struct Money {
  std::string currency;
  Decimal amount;

  Money() = default;
  Money(std::string cur, Decimal a) : currency(std::move(cur)), amount(a) {}
  explicit Money(const pb::MoneyValue& m)
      : currency(m.currency()), amount(m.units(), m.nano()) {}

  pb::MoneyValue to_pb() const { return amount.to_money(currency); }
  std::string to_string() const { return amount.to_string() + " " + currency; }

  friend bool operator==(const Money& a, const Money& b) {
    return a.currency == b.currency && a.amount == b.amount;
  }
};

}  // namespace tinvest
