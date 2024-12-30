//
// Created by yyx on 2023/3/8.
//

#ifndef ALT_INDEX_LINEAR_MODEL_H
#define ALT_INDEX_LINEAR_MODEL_H

#include <limits>
#include <cmath>
#include <cstdlib>
#include <algorithm>

namespace alt_index
{
    template <class key_type>
    class LinearModel
    {
    public:
        LinearModel() = default;

        LinearModel(double a, double b) : a(a), b(b) {}

        explicit LinearModel(const LinearModel &other) : a(other.a), b(other.b) {}

        ~LinearModel() {}

        inline int predict(key_type key) const
        {
            return static_cast<int>(a * static_cast<double>(key) + b);
        }

        inline double predict_double(key_type key) const
        {
            return a * static_cast<double>(key) + b;
        }

        // variable type is decided
        double a;
        double b;
    };
}

#endif // ALT_INDEX_LINEAR_MODEL_H
