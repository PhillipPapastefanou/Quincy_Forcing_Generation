#!/bin/bash
#SBATCH -J GUESS_NCPS
#SBATCH --error=%j.err
#SBATCH --output=%j.log
#SBATCH -D ./
#SBATCH --get-user-env
#SBATCH --mail-type=ALL
#SBATCH --mail-user=papa@tum.de
#SBATCH --time=24:00:00
#SBATCH --nodes=2
#SBATCH --export=NONE
#SBATCH --ntasks=128
#SBATCH --partition='work'
#SBATCH --mem='400G'
module purge
ml intel/2023.0.0 impi/2021.6.0
ml netcdf/4.9.0
ml all/Miniconda3


source /User/homes/ppapastefanou/miniconda3/etc/profile.d/conda.sh
#conda info --envs
conda activate /Net/Groups/BSI/work/quincy/model/quincy_scripts/netcdf_postprocessing/qnc_lib_py_env
which python

export I_MPI_PIN_RESPECT_CPUSET=0
export FI_PROVIDER=tcp



mpiexec -n 128 python -u Cluster_Quincy_forcing_generation.py
