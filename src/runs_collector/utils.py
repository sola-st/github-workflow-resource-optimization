from multiprocessing import Pool

def parallel_list_processing(data, func, n_cpus = 48):
    
    pool = Pool()
    len_items = len(data)
    results_cpu = []
        
    for i in range(n_cpus):
        result_i = pool.apply_async(func, [data[int((i)*len_items/n_cpus):int((i+1)*len_items/n_cpus)]])
        results_cpu.append(result_i)

    answers_cpu = []
    for result_i in results_cpu:
        answers_cpu.append(result_i.get())
    final_answer = {}

    for answer_i in answers_cpu:
        final_answer.update(answer_i)
    
    return final_answer


def parallel_commits_fetching(commits_list, repo, func, n_cpus = 48):
    
    pool = Pool()
    len_items = len(commits_list)
    results_cpu = []
        
    for i in range(n_cpus):
        result_i = pool.apply_async(func, [commits_list[int((i)*len_items/n_cpus):int((i+1)*len_items/n_cpus)], repo])
        results_cpu.append(result_i)

    answers_cpu = []
    for result_i in results_cpu:
        answers_cpu.append(result_i.get())
    final_answer = {
                    "Added": [],
                    "Deleted": [],
                    "Modified": [],
                    "Renamed": []
                }

    for answer_i in answers_cpu:
        final_answer["Added"].extend(answer_i["Added"])
        final_answer["Deleted"].extend(answer_i["Deleted"])
        final_answer["Modified"].extend(answer_i["Modified"])
        final_answer["Renamed"].extend(answer_i["Renamed"])
    
    return final_answer