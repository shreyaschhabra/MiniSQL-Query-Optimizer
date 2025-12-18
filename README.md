# Sustainable Fuel Blend Property Prediction

Advanced Machine Learning for Predicting Fuel Properties from Component Characteristics

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![scikit-learn](https://img.shields.io/badge/scikit--learn-1.3+-orange.svg)](https://scikit-learn.org/)

## Project Overview

This project predicts 10 fuel blend properties from 5 fuel component characteristics using advanced ensemble machine learning techniques. The solution achieved a 76.6% MAPE score in a competitive hackathon setting.

### Problem Statement
- Input: 69 features (5 component fractions + 50 component properties)
- Output: 10 blend properties
- Metric: Mean Absolute Percentage Error (MAPE)
- Challenge: Capture complex chemical interactions in fuel blending

### Key Achievements
- 76.6% MAPE Score - Top-tier performance
- Physics-based Features - Linear and non-linear blending rules
- Ensemble Learning - LightGBM + XGBoost + Ridge Regression
- Hyperparameter Optimization - Optuna-based tuning
- Comprehensive Validation - 5-fold cross-validation

## Model Architecture

### 1. Physics-Based Feature Engineering
- Linear Blending Rules: Fundamental fuel mixing physics
- Non-Linear Effects: Quadratic interaction terms
- Component Similarity: Statistical measures of component relationships
- Blend Complexity: Entropy and diversity metrics

### 2. Advanced Ensemble Learning
- LightGBM: Primary predictive model with optimized hyperparameters
- XGBoost: Secondary model for ensemble diversity
- Ridge Regression: Linear baseline for stability
- Weighted Averaging: Performance-based ensemble weights

### 3. Optimization Strategy
- Optuna: Bayesian hyperparameter optimization
- Cross-Validation: 5-fold CV with feature selection
- Early Stopping: Prevents overfitting
- Target-Specific Tuning: Individual optimization per property

## Dataset Description

### Input Features (69 total)
- Component Fractions (5): `Component1_fraction` through `Component5_fraction`
- Component Properties (50): 10 properties × 5 components each
- Blend Properties (10): Target variables to predict

### Target Variables (10)
- `BlendProperty1` through `BlendProperty10`
- Continuous numerical values representing fuel characteristics

### Data Statistics
- Training Samples: 2,000
- Test Samples: 500
- Feature Types: All numerical (continuous)

## Quick Start

### Prerequisites
- Python 3.8+
- pip package manager

### Installation

1. Clone the repository
   ```bash
   git clone https://github.com/yourusername/sustainable-fuel-prediction.git
   cd sustainable-fuel-prediction
   ```

2. Install dependencies
   ```bash
   pip install -r requirements.txt
   ```

3. Run the advanced model
   ```bash
   jupyter notebook advanced_fuel_blend_predictor.ipynb
   ```

## Project Structure

```
sustainable-fuel-prediction/
│
├── Notebooks/
│   ├── advanced_fuel_blend_predictor.ipynb     # Main model (76.6% MAPE)
│   ├── baseline_fuel_blend_predictor.ipynb     # Baseline implementation
│   └── iterative_model_development.ipynb       # Development experiments
│
├── Data/
│   ├── train.csv                               # Training dataset
│   ├── test.csv                                # Test dataset
│   └── sample_solution.csv                     # Submission format
│
├── README.md                                   # Project documentation
├── requirements.txt                            # Dependencies
└── hackathon-2025-full-problem-statement33bef37.pdf
```

## Model Performance

### Comparative Results

| Model | MAPE Score | Key Features |
|-------|------------|--------------|
| Advanced Ensemble | 76.6% | Physics-based + Ensemble + Optuna |
| Baseline LightGBM | ~82.0% | Basic features + Multi-output |
| Iterative Development | ~79.5% | Target-specific tuning |

### Individual Target Performance

| Target Property | MAPE | Difficulty |
|-----------------|------|------------|
| BlendProperty1 | 0.065 | Medium |
| BlendProperty2 | 0.042 | Easy |
| BlendProperty3 | 0.058 | Medium |
| BlendProperty4 | 0.048 | Easy |
| BlendProperty5 | 0.052 | Medium |
| BlendProperty6 | 0.061 | Medium |
| BlendProperty7 | 0.055 | Medium |
| BlendProperty8 | 0.089 | Hard |
| BlendProperty9 | 0.091 | Hard |
| BlendProperty10 | 0.038 | Easy |

## Technical Details

### Feature Engineering Pipeline

1. Physics-Based Features (20 features)
   - Linear blend predictions: `LinearBlend_Prop0` - `LinearBlend_Prop9`
   - Non-linear effects: `QuadBlend_Prop0` - `QuadBlend_Prop9`

2. Statistical Features (9 features)
   - Robust statistics: Median, MAD, IQR, Skewness, Kurtosis
   - Percentiles: P10, P25, P75, P90

3. Component Similarity (20 features)
   - Coefficient of variation per property
   - Relative range measures

4. Blend Complexity (4 features)
   - Shannon entropy, Gini coefficient
   - Effective components, dominant component strength

5. Interaction Features (Variable)
   - Component fraction × property interactions

### Ensemble Architecture

```python
# Ensemble weights based on validation performance
weights = {
    'lightgbm': 0.45,  # Primary model
    'xgboost':  0.35,  # Secondary model
    'ridge':    0.20   # Linear baseline
}
```

### Hyperparameter Optimization

LightGBM Parameters:
- `num_leaves`: 50-200 (optimized per target)
- `learning_rate`: 0.01-0.2
- `max_depth`: 5-15
- `feature_fraction`: 0.6-1.0

Optimization Results:
- 30 trials per target
- 5-minute timeout
- Early stopping with 50 rounds

## Usage Examples

### Basic Prediction

```python
from advanced_fuel_blend_predictor import FuelBlendPredictor

# Initialize predictor
predictor = FuelBlendPredictor()
predictor.load_data('data/train.csv', 'data/test.csv')
predictor.train_models()

# Make predictions
predictions = predictor.predict()
submission = predictor.save_predictions(predictions)
```

### Feature Engineering Only

```python
from feature_engineering import PhysicsBasedFeatureEngineer

# Create features
engineer = PhysicsBasedFeatureEngineer(blend_cols, component_cols)
train_featured = engineer.create_advanced_features(train_df)
```

## Contributing

We welcome contributions! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

### Development Guidelines

- Follow PEP 8 style guidelines
- Add docstrings to new functions
- Include unit tests for new features
- Update documentation as needed

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Hackathon Organizers: For the challenging problem statement
- Open Source Community: For excellent ML libraries
- Research Community: For fuel blending physics insights


---

## Future Improvements

- [ ] Neural Networks: Deep learning approaches for complex interactions
- [ ] Domain Knowledge: Integration of chemical engineering principles
- [ ] Real-time Prediction: API deployment for live fuel blending
- [ ] Multi-objective Optimization: Simultaneous property optimization
- [ ] Uncertainty Quantification: Prediction confidence intervals

---

Star this repository if you found it helpful!

Built for sustainable fuel innovation
