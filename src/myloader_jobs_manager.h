/*
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

        Authors:    David Ducos, Percona (david dot ducos at percona dot com)
*/

//enum purge_mode { NONE, DROP, TRUNCATE, DELETE };

//struct job * new_job (enum job_type type, void *job_data, char *use_database);
//gboolean process_job(struct thread_data *td, struct job *job);
void initialize_job();
void *loader_thread(struct thread_data *td);
void *signal_thread(void *data);
void initialize_loader_threads(struct configuration *conf);
void wait_loader_threads_to_finish();
void free_loader_threads();
