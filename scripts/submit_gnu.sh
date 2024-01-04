#!/bin/bash
#SBATCH -J GUESS_NCPS
#SBATCH --error=%j.err
#SBATCH --output=%j.log
#SBATCH -D ./
#SBATCH --get-user-env
#SBATCH --mail-type=ALL
#SBATCH --export=NONE
#SBATCH --time=24:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=32
#SBATCH --partition='work'
#SBATCH --mem='100G'
module purge
ml all/Miniconda3
#ml intel/2023.0.0 impi/2021.6.0
#ml gnu12/12.2.0  openmpi4/4.1.4 py3-mpi4py/3.1.3
#ml netcdf/4.9.0
#ml py3-mpi4py/3.1.3
source /User/homes/ppapastefanou/miniconda3/etc/profile.d/conda.sh
#conda info --envs
conda activate /Net/Groups/BSI/work/quincy/model/quincy_scripts/netcdf_postprocessing/qnc_lib_py_env
which python
which mpicc
#ml intel/2023.0.0 impi/2021.6.0
#ml py3-mpi4py/3.1.3
ml gnu12/12.2.0  openmpi4/4.1.4
#ml py3-mpi4py/3.1.3
#ml netcdf/4.9.0
which python
which mpicc
which orted
which mpicc
export FI_PROVIDER=tcp

mpirun -n 32 python Cluster_Quincy_forcing_generation.py
