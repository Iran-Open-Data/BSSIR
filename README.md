# BSSIR — Basic Survey Structure for Iran

BSSIR is a foundational Python package developed by Iran-Open-Data that provides shared data-loading and transformation functionality for a variety of socioeconomic surveys from Iran. It serves as the common infrastructure for more specialized data-analysis packages.

## Overview

Working with raw survey data from Iran can be challenging. Datasets often come in complex formats with inconsistencies in naming conventions, value codes, and structures that change over the years. BSSIR tackles these foundational challenges by providing a robust, standardized layer for data access and processing.

This package handles the essential behind-the-scenes work of reading, cleaning, and standardizing data, allowing researchers and developers to build more specific analysis tools on a reliable and consistent foundation.

## Core features

- **Standardized data loading**: Read complex raw survey files from different years without worrying about underlying formats.  
- **Schema standardization**: Apply consistent schemas and naming conventions across survey years.  
- **Metadata integration**: Integrate essential metadata (regional and geographical attributes) into datasets.  
- **Configuration management**: Simple system for managing data paths, settings, and configurations.  
- **Foundational layer**: Acts as the base dependency for other *SIR packages, ensuring code reuse and consistency.

## The *SIR ecosystem

BSSIR is the core component of a larger ecosystem designed to make Iranian socioeconomic data accessible. It provides the base infrastructure for more specialized packages so they can focus on dataset-specific analysis.

- **BSSIR**: Core data-handling infrastructure.  
- **HBSIR**: Builds on BSSIR to provide tools for analyzing the Household Budget Survey.  
- **LFSIR**: Adds provisions for the Labor Force Survey.  
- **CNSIR**: Builds on BSSIR to provide tools for analyzing Iran's Census data.
