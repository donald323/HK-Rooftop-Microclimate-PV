"""Centralized matplotlib/Seaborn styling for PVIGR figures."""

import matplotlib.pyplot as plt
import seaborn as sns


def apply_plot_style() -> None:
    """Apply the standard plotting style used across notebooks and modules."""
    plt.rc('font', family='serif')
    plt.rc('axes', labelsize=16)
    plt.rc('xtick', labelsize=14, color='grey')
    plt.rc('ytick', labelsize=14, color='grey')
    plt.rc('legend', fontsize=16, loc='lower left')
    plt.rc('figure', titlesize=18)
    plt.rc('savefig', dpi=330, bbox='tight')
    sns.set_style('white')
