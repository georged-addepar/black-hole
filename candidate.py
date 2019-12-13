class Candidate:
    def __init__(self, application_id, candidate_id, first_name, last_name, applied_at, company, title, job_name,
                 file_name, url):
        self.application_id = application_id
        self.candidate_id = candidate_id
        self.first_name = first_name
        self.last_name = last_name
        self.applied_at = applied_at
        self.company = company
        self.title = title
        self.job_name = job_name
        self.file_name = file_name
        self.url = url
        self.resume = None

    def __repr__(self):
        return 'candidate id %s file name %s' % (self.candidate_id, self.file_name)
