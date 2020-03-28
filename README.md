# cs342spring2020-p1
## Description

This files includdes my (ID: 21701546) submission for cs342 (Operating Systems course) project 1 in Spring 2020. The processes multiplies a matrix by a vector and saves the result into a file. The way the multiplication is done is dependent on the process.

## Usage
To compile, do:

'''
make
'''

To run,  

'''
<process_name> <matrix_file> <vector_file> <result_fule>
'''

where <process_name> is one of the following:
- mv: uses temporary files as intermediate buffers
- mvp: uses pipes as intermediate buffers
- mvt: uses threads


